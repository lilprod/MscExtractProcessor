using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Concurrent;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

/// <summary>
/// Service de fond qui orchestre le scan initial et le traitement en temps réel.
/// </summary>
public class Worker : BackgroundService
{
    private readonly ILogger<Worker> _logger;
    private readonly FileProcessor _processor;

    private FileSystemWatcher _watcher;
    private string _currentWatchedYear;

    // Queue pour stocker les fichiers détectés par le Watcher (mémoire tampon)
    private readonly ConcurrentQueue<string> _fileQueue = new();
    private readonly SemaphoreSlim _queueSemaphore = new(0);

    // Registre pour éviter de traiter deux fois le même fichier (Scan initial vs Watcher)
    private readonly ConcurrentDictionary<string, byte> _processedFiles = new();

    public Worker(ILogger<Worker> logger, FileProcessor processor)
    {
        _logger = logger;
        _processor = processor;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("=== INITIALISATION DU SERVICE MSC EXTRACT ===");

        // 1. Activer le Watcher immédiatement sur l'année en cours
        StartWatcher();

        // 2. Lancement du Scan Initial Séquentiel (Rattrapage des fichiers existants)
        _logger.LogInformation("Début du traitement séquentiel des dossiers existants...");
        await _processor.ProcessInitialFoldersSequentiallyAsync(_processedFiles, stoppingToken);

        _logger.LogInformation("=== SCAN INITIAL TERMINÉ. PASSAGE AU TRAITEMENT TEMPS RÉEL ===");

        // 3. Boucle principale de traitement
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                // Vérification du changement d'année (ex: passage de 2026 à 2027)
                CheckAndHandleYearChange();

                // Nettoyage quotidien de la mémoire à minuit (pour garder le service léger)
                CleanProcessedCacheAtMidnight();

                // Attente d'un signal de la file d'attente
                await _queueSemaphore.WaitAsync(stoppingToken);

                if (_fileQueue.TryDequeue(out var filePath))
                {
                    // Sécurité anti-doublon (si le scan initial et le watcher voient le même fichier)
                    if (_processedFiles.TryAdd(filePath, 0))
                    {
                        _logger.LogInformation($"[TEMPS RÉEL] Nouveau fichier : {Path.GetFileName(filePath)}");

                        // On attend que le système source ait fini d'écrire le fichier
                        await WaitForFileReady(filePath);

                        // Traitement métier via le processeur
                        await _processor.ProcessFileIfEligibleAsync(filePath);
                    }
                }
            }
            catch (OperationCanceledException) { break; }
            catch (Exception ex)
            {
                _logger.LogError($"Erreur dans la boucle Worker : {ex.Message}");
            }
        }
    }

    /// <summary>
    /// Initialise ou redémarre le Watcher sur le dossier de l'année.
    /// </summary>
    private void StartWatcher()
    {
        string yearToWatch = DateTime.Now.ToString("yyyy");

        // Si c'est le premier démarrage, on respecte l'année de StartDateHourMin si fournie
        if (string.IsNullOrEmpty(_currentWatchedYear) && _processor.Config.StartDateHourMin.Length >= 4)
        {
            yearToWatch = _processor.Config.StartDateHourMin[..4];
        }

        string path = Path.Combine(_processor.Config.DossierSource, yearToWatch);

        if (!Directory.Exists(path))
        {
            _logger.LogError($"Dossier cible introuvable : {path}. Le Watcher attendra une création manuelle ou le prochain cycle.");
            return;
        }

        // Nettoyage de l'ancien Watcher
        if (_watcher != null)
        {
            _watcher.EnableRaisingEvents = false;
            _watcher.Dispose();
        }

        _currentWatchedYear = yearToWatch;
        _watcher = new FileSystemWatcher(path, "*.gz")
        {
            EnableRaisingEvents = true,
            IncludeSubdirectories = true, // Permet de surveiller 02, 03, 04... automatiquement
            NotifyFilter = NotifyFilters.FileName | NotifyFilters.Size | NotifyFilters.LastWrite
        };

        _watcher.Created += (s, e) => EnqueueFile(e.FullPath);
        _watcher.Changed += (s, e) => EnqueueFile(e.FullPath);

        // Gestion robuste des erreurs réseau ou système
        _watcher.Error += (s, e) => {
            _logger.LogWarning($"Watcher Error: {e.GetException().Message}. Restarting in 10s...");
            Task.Delay(10000).ContinueWith(_ => StartWatcher());
        };

        _logger.LogInformation($"Watcher actif sur l'année complète : {path}");
    }

    /// <summary>
    /// Filtre et ajoute un fichier à la file d'attente de traitement.
    /// </summary>
    private void EnqueueFile(string path)
    {
        try
        {
            // Vérification métier immédiate (préfixe, suffixe, date)
            if (_processor.IsFileEligible(path))
            {
                if (File.Exists(path) && new FileInfo(path).Length > 0)
                {
                    _fileQueue.Enqueue(path);
                    _queueSemaphore.Release();
                }
            }
        }
        catch { /* Ignoré : le fichier est peut-être en cours de verrouillage par l'OS */ }
    }

    /// <summary>
    /// Boucle d'attente pour s'assurer que le fichier n'est plus utilisé par le MSC.
    /// </summary>
    private async Task WaitForFileReady(string path)
    {
        int attempts = 0;
        while (attempts < 15)
        {
            try
            {
                using var fs = File.Open(path, FileMode.Open, FileAccess.Read, FileShare.None);
                return; // Fichier libre
            }
            catch (IOException)
            {
                attempts++;
                await Task.Delay(2000); // Attente de 2 secondes
            }
        }
        _logger.LogWarning($"Le fichier {path} est resté verrouillé trop longtemps.");
    }

    private void CheckAndHandleYearChange()
    {
        string currentYear = DateTime.Now.ToString("yyyy");
        if (_currentWatchedYear != null && currentYear != _currentWatchedYear)
        {
            _logger.LogInformation($"Changement d'année détecté : {_currentWatchedYear} -> {currentYear}");
            StartWatcher();
        }
    }

    private void CleanProcessedCacheAtMidnight()
    {
        // Si le cache dépasse 50 000 entrées, on le vide pour libérer la RAM
        // (Les doublons seront de toute façon gérés par LastProcessed.txt)
        if (_processedFiles.Count > 50000)
        {
            _logger.LogInformation("Nettoyage préventif du cache des fichiers traités.");
            _processedFiles.Clear();
        }
    }
}