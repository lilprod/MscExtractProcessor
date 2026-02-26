using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Concurrent;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

public class Worker : BackgroundService
{
    private readonly ILogger<Worker> _logger;
    private readonly FileProcessor _processor;

    private FileSystemWatcher _watcher;
    private string _currentWatchedYear;

    // Queue haute performance pour le flux massif de fichiers
    private readonly ConcurrentQueue<string> _fileQueue = new();
    private readonly SemaphoreSlim _queueSemaphore = new(0);

    // Registre anti-doublon pour éviter les conflits Scan/Watcher
    private readonly ConcurrentDictionary<string, byte> _processedFiles = new();

    public Worker(ILogger<Worker> logger, FileProcessor processor)
    {
        _logger = logger;
        _processor = processor;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("=== INITIALISATION DU SERVICE MSC EXTRACT (MODE HAUTE DISPONIBILITÉ) ===");

        // 1. Initialisation de la surveillance temps réel
        StartWatcher();

        // 2. Rattrapage des fichiers existants (Scan initial)
        _logger.LogInformation("Début du scan de rattrapage des dossiers...");
        await _processor.ProcessInitialFoldersSequentiallyAsync(_processedFiles, stoppingToken);

        _logger.LogInformation("=== SCAN INITIAL TERMINÉ. TRAITEMENT DU FLUX EN COURS ===");

        // 3. Boucle principale de consommation de la file d'attente
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                CheckAndHandleYearChange();
                CleanProcessedCacheAtMidnight();

                // Attente d'un nouveau fichier dans la file
                await _queueSemaphore.WaitAsync(stoppingToken);

                if (_fileQueue.TryDequeue(out var filePath))
                {
                    // Sécurité anti-doublon
                    if (_processedFiles.TryAdd(filePath, 0))
                    {
                        // On attend que le fichier soit totalement écrit/libéré par le MSC
                        await WaitForFileReady(filePath);

                        // Vérification finale de la taille (> 1 Ko) avant traitement métier
                        var fileInfo = new FileInfo(filePath);
                        if (fileInfo.Exists && fileInfo.Length > 1024)
                        {
                            string type = filePath.Contains("SMSinMSC", StringComparison.OrdinalIgnoreCase) ? "SMS" : "VOIX";
                            _logger.LogInformation($"[TRAITEMENT] {type} détecté : {fileInfo.Name} ({fileInfo.Length / 1024} Ko)");

                            await _processor.ProcessFileIfEligibleAsync(filePath);
                        }
                        else if (fileInfo.Exists)
                        {
                            _logger.LogDebug($"[IGNORÉ] Fichier trop petit (< 1Ko) : {fileInfo.Name}");
                        }
                    }
                }
            }
            catch (OperationCanceledException) { break; }
            catch (Exception ex)
            {
                _logger.LogError($"Erreur boucle principale : {ex.Message}");
            }
        }
    }

    private void StartWatcher()
    {
        string yearToWatch = DateTime.Now.ToString("yyyy");
        if (string.IsNullOrEmpty(_currentWatchedYear) && _processor.Config.StartDateHourMin.Length >= 4)
            yearToWatch = _processor.Config.StartDateHourMin[..4];

        string path = Path.Combine(_processor.Config.DossierSource, yearToWatch);

        if (!Directory.Exists(path))
        {
            _logger.LogError($"Dossier source introuvable : {path}");
            return;
        }

        if (_watcher != null)
        {
            _watcher.EnableRaisingEvents = false;
            _watcher.Dispose();
        }

        _currentWatchedYear = yearToWatch;

        // Configuration "Gros Volume"
        _watcher = new FileSystemWatcher(path, "*.*")
        {
            IncludeSubdirectories = true,
            InternalBufferSize = 65536, // Buffer maximum (64Ko) pour 100K fichiers
            NotifyFilter = NotifyFilters.FileName | NotifyFilters.LastWrite,
            EnableRaisingEvents = true
        };

        _watcher.Created += (s, e) => EnqueueFile(e.FullPath);
        _watcher.Changed += (s, e) => EnqueueFile(e.FullPath);
        _watcher.Renamed += (s, e) => EnqueueFile(e.FullPath);

        _watcher.Error += (s, e) => {
            _logger.LogWarning($"Watcher Error: {e.GetException().Message}. Restarting...");
            Task.Delay(5000).ContinueWith(_ => StartWatcher());
        };

        _logger.LogInformation($"Watcher actif (Buffer 64Ko) sur : {path}");
    }

    private void EnqueueFile(string path)
    {
        try
        {
            // On ignore les dossiers
            if (Directory.Exists(path)) return;

            string fileName = Path.GetFileName(path);

            // Récupération dynamique des suffixes autorisés depuis le appsettings
            var allowedSuffixes = _processor.Config.SuffixesFichiers
                .Split(',', StringSplitOptions.RemoveEmptyEntries)
                .Select(s => s.Trim())
                .ToList();

            // Filtrage ultra-rapide par suffixe pour ne pas saturer la file
            if (allowedSuffixes.Any(s => fileName.EndsWith(s, StringComparison.OrdinalIgnoreCase)))
            {
                _fileQueue.Enqueue(path);
                _queueSemaphore.Release();
            }
        }
        catch { /* Erreur silencieuse pour ne pas bloquer le thread du Watcher */ }
    }

    private async Task WaitForFileReady(string path)
    {
        int attempts = 0;
        while (attempts < 20) // Augmenté à 20 tentatives pour les gros fichiers voix
        {
            try
            {
                using var fs = File.Open(path, FileMode.Open, FileAccess.Read, FileShare.None);
                return;
            }
            catch (IOException)
            {
                attempts++;
                await Task.Delay(2000);
            }
        }
        _logger.LogWarning($"Fichier verrouillé trop longtemps (ignoré) : {path}");
    }

    private void CheckAndHandleYearChange()
    {
        string currentYear = DateTime.Now.ToString("yyyy");
        if (_currentWatchedYear != null && currentYear != _currentWatchedYear)
        {
            _logger.LogInformation($"Nouvelle année : {currentYear}. Redémarrage Watcher.");
            StartWatcher();
        }
    }

    private void CleanProcessedCacheAtMidnight()
    {
        if (_processedFiles.Count > 100000) // Seuil adapté à votre volume de 100K
        {
            _logger.LogInformation("Nettoyage du cache anti-doublon (100K+ entrées).");
            _processedFiles.Clear();
        }
    }
}