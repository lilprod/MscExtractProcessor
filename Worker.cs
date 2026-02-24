using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System.Collections.Concurrent;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

public class Worker : BackgroundService
{
    private readonly ILogger<Worker> _logger;
    private readonly FileProcessor _processor;
    private FileSystemWatcher _watcher;

    // Queue pour stocker les fichiers détectés par le Watcher pendant le scan initial
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
        _logger.LogInformation("=== DÉMARRAGE DU SERVICE MSC EXTRACT ===");

        // 1. On active le Watcher immédiatement. 
        // Les nouveaux fichiers arrivant maintenant seront mis en queue (mémoire tampon).
        StartWatcher();

        // 2. Lancement du Scan Initial Séquentiel (Dossier par Dossier)
        // Cette méthode est bloquante : elle traite le jour 01, puis 02, etc.
        _logger.LogInformation("Début du traitement séquentiel des dossiers existants...");
        await _processor.ProcessInitialFoldersSequentiallyAsync(_processedFiles, stoppingToken);

        _logger.LogInformation("=== SCAN INITIAL TERMINÉ. PASSAGE AU TRAITEMENT TEMPS RÉEL ===");

        // 3. Boucle infinie pour traiter la file d'attente (fichiers arrivés pendant le scan)
        while (!stoppingToken.IsCancellationRequested)
        {
            // On attend qu'un fichier soit disponible dans la queue
            await _queueSemaphore.WaitAsync(stoppingToken);

            if (_fileQueue.TryDequeue(out var filePath))
            {
                // On vérifie si le fichier n'a pas déjà été traité lors du scan initial
                if (_processedFiles.TryAdd(filePath, 0))
                {
                    _logger.LogInformation($"[TEMPS RÉEL] Nouveau fichier détecté : {Path.GetFileName(filePath)}");

                    // Sécurité : on attend que le fichier soit totalement écrit sur le disque
                    await WaitForFileReady(filePath);

                    // Traitement du fichier
                    await _processor.ProcessFileIfEligibleAsync(filePath);
                }
            }
        }
    }

    private void StartWatcher()
    {
        string currentYear = DateTime.Now.ToString("yyyy");
        string folderYear = _processor.Config.StartDateHourMin.Length >= 4 ? _processor.Config.StartDateHourMin[..4] : currentYear;
        string folderToWatch = _processor.Config.StartDateHourMin.Length >= 6 ? _processor.Config.StartDateHourMin.Substring(4, 2) : DateTime.Now.ToString("MM");

        string path = Path.Combine(_processor.Config.DossierSource, folderYear, folderToWatch);

        if (!Directory.Exists(path))
        {
            _logger.LogError($"Impossible de démarrer le Watcher : le chemin {path} n'existe pas.");
            return;
        }

        _watcher = new FileSystemWatcher(path, "*.gz")
        {
            EnableRaisingEvents = true,
            IncludeSubdirectories = true,
            NotifyFilter = NotifyFilters.FileName | NotifyFilters.Size | NotifyFilters.LastWrite
        };

        _watcher.Created += (s, e) => EnqueueFile(e.FullPath);
        _watcher.Changed += (s, e) => EnqueueFile(e.FullPath);

        _logger.LogInformation($"Watcher actif sur : {path}");
    }

    private void EnqueueFile(string path)
    {
        // On ignore les fichiers vides en cours de création
        if (File.Exists(path) && new FileInfo(path).Length > 0)
        {
            _fileQueue.Enqueue(path);
            _queueSemaphore.Release(); // Réveille la boucle de traitement
        }
    }

    private async Task WaitForFileReady(string path)
    {
        int attempts = 0;
        while (attempts < 10)
        {
            try
            {
                // Tente d'ouvrir le fichier avec un accès exclusif
                using var fs = File.Open(path, FileMode.Open, FileAccess.Read, FileShare.None);
                return;
            }
            catch (IOException)
            {
                attempts++;
                await Task.Delay(2000); // Attend 2 secondes avant de réessayer
            }
        }
        _logger.LogWarning($"Le fichier {path} semble toujours verrouillé après plusieurs tentatives.");
    }
}