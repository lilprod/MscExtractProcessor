using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

/// <summary>
/// Classe responsable du traitement métier des fichiers MSC : 
/// Filtrage, Décompression, Extraction de données, Compression et Envoi FTP.
/// </summary>
public class FileProcessor
{
    private readonly ExtractConfig _config;
    private readonly ILogger<FileProcessor> _logger;
    private readonly FtpHelper _ftp;
    private readonly HashSet<string> _fileB;
    private readonly Dictionary<string, DailyStats> _stats = new();

    // Suivi par clé composée : "PREFIXE|SUFFIXE" (ex: "LOMBC1_|msOriginating")
    // Permet de traiter msOriginating et msOriginatingSMSinMSC indépendamment.
    private readonly ConcurrentDictionary<string, string> _lastProcessedTracker = new();

    // Verrous par fichier pour éviter les conflits d'accès entre les threads.
    private readonly ConcurrentDictionary<string, SemaphoreSlim> _fileLocks = new();

    public ExtractConfig Config => _config;

    public FileProcessor(IOptions<ExtractConfig> config, ILogger<FileProcessor> logger, FtpHelper ftp)
    {
        _config = config.Value;
        _logger = logger;
        _ftp = ftp;

        // Initialisation du répertoire de sauvegarde de l'état
        Directory.CreateDirectory(Path.GetDirectoryName(_config.LastProcessedPath));
        LoadLastProcessed();

        // Chargement du fichier de filtrage B (MSISDN cibles)
        if (File.Exists(_config.FileBPath))
            _fileB = new HashSet<string>(File.ReadAllLines(_config.FileBPath));
        else
            _fileB = new HashSet<string>();
    }

    /// <summary>
    /// Scanne récursivement tous les mois et jours de l'année configurée.
    /// Traite les fichiers présents avant le démarrage du service.
    /// </summary>
    public async Task ProcessInitialFoldersSequentiallyAsync(ConcurrentDictionary<string, byte> processedFiles, CancellationToken ct)
    {
        string currentYear = DateTime.Now.ToString("yyyy");
        string folderYear = _config.StartDateHourMin.Length >= 4 ? _config.StartDateHourMin[..4] : currentYear;
        string rootYearPath = Path.Combine(_config.DossierSource, folderYear);

        if (!Directory.Exists(rootYearPath))
        {
            _logger.LogWarning($"Répertoire racine introuvable : {rootYearPath}");
            return;
        }

        // Itération sur les dossiers de mois (01, 02, 03...)
        foreach (var monthDir in Directory.EnumerateDirectories(rootYearPath).OrderBy(m => m))
        {
            // Itération sur les dossiers de jours (01, 02...)
            foreach (var dayDir in Directory.EnumerateDirectories(monthDir).OrderBy(d => d))
            {
                if (ct.IsCancellationRequested) break;

                // On traite les fichiers .gz par ordre alphabétique
                var files = Directory.EnumerateFiles(dayDir, "*.gz").OrderBy(f => f).ToList();
                foreach (var file in files)
                {
                    if (IsFileEligible(file))
                    {
                        _logger.LogInformation($"      [OK] Détecté (Scan) : {Path.GetFileName(file)}");
                        await ProcessFileIfEligibleAsync(file);
                        processedFiles.TryAdd(file, 0); // Marqué comme traité pour le Worker
                    }
                }
            }
        }
    }

    /// <summary>
    /// Vérifie l'éligibilité d'un fichier et gère le verrouillage avant traitement.
    /// </summary>
    public async Task ProcessFileIfEligibleAsync(string filePath)
    {
        if (!IsFileEligible(filePath)) return;

        // Verrouillage par fichier pour éviter les accès concurrents
        var fileLock = _fileLocks.GetOrAdd(filePath, new SemaphoreSlim(1, 1));
        await fileLock.WaitAsync();

        try
        {
            string fileName = Path.GetFileName(filePath);
            string date = ExtractDate(fileName);

            _logger.LogInformation($"      [TRAITEMENT] {fileName}");
            await ProcessFileAsync(filePath, fileName, date);

            // Mise à jour du registre de dernier fichier traité
            UpdateLastProcessed(fileName);
        }
        finally
        {
            fileLock.Release();
        }
    }

    /// <summary>
    /// Logique principale : Décompression, filtrage métier, re-compression et archivage.
    /// </summary>
    private async Task ProcessFileAsync(string path, string fileName, string date)
    {
        string key = date[..8]; // Clé YYYYMMDD pour les stats
        if (!_stats.ContainsKey(key)) _stats[key] = new DailyStats();
        _stats[key].Detected++;

        // Création d'un dossier temporaire unique pour la décompression
        string tempDir = Path.Combine(Path.GetTempPath(), "MscExtract_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(tempDir);

        try
        {
            string txtFile = Path.Combine(tempDir, fileName.Replace(".gz", ""));
            Decompress(path, txtFile);

            // Choix du filtre en fonction du type de fichier
            bool isMatch = fileName.Contains("_msOriginating")
                ? await ProcessMsOriginating(txtFile)
                : await ProcessSmsMsc(txtFile);

            if (!isMatch)
            {
                _stats[key].Deleted++;
                return;
            }

            _stats[key].Processed++;

            // Préparation du dossier de sortie (Année/Mois/Jour)
            string outDir = Path.Combine(_config.OutputDirectory, date[..4], date.Substring(4, 2), date.Substring(6, 2));
            Directory.CreateDirectory(outDir);

            string finalPath = Path.Combine(outDir, fileName);
            Compress(txtFile, finalPath);

            // Envoi FTP si activé
            if (_config.SendFileFTP.Equals("oui", StringComparison.OrdinalIgnoreCase))
                HandleFtpSendingWithRetry(finalPath, key);
        }
        catch (Exception ex)
        {
            _logger.LogError($"Erreur lors du traitement de {fileName} : {ex.Message}");
        }
        finally
        {
            // Nettoyage sécurisé du répertoire temporaire
            try { if (Directory.Exists(tempDir)) Directory.Delete(tempDir, true); }
            catch { /* Suppression différée si le fichier est encore verrouillé par l'OS */ }
        }
    }

    /// <summary>
    /// Détermine si un fichier doit être traité en fonction de son nom, de sa date et de l'historique.
    /// </summary>
    private bool IsFileEligible(string filePath)
    {
        string fileName = Path.GetFileName(filePath);
        string fileDate = ExtractDate(fileName);
        if (fileDate == null) return false;

        // Analyse du préfixe et du suffixe
        var prefixes = _config.PrefixesFichiers.Split(',').Select(p => p.Trim()).ToList();
        var suffixes = _config.SuffixesFichiers.Split(',').Select(s => s.Trim()).ToList();

        string currentPrefix = prefixes.FirstOrDefault(p => fileName.StartsWith(p, StringComparison.OrdinalIgnoreCase));
        string currentSuffix = suffixes.FirstOrDefault(s => fileName.EndsWith(s, StringComparison.OrdinalIgnoreCase));

        // Filtrage de base (format, existence, taille)
        if (currentPrefix == null || currentSuffix == null) return false;
        if (!File.Exists(filePath) || new FileInfo(filePath).Length == 0) return false;

        // Vérification par rapport au dernier fichier traité pour CE préfixe ET CE suffixe
        string trackingKey = $"{currentPrefix}|{currentSuffix}";

        if (_lastProcessedTracker.TryGetValue(trackingKey, out string lastFile))
        {
            // Élimine les doublons de notification
            if (fileName.Equals(lastFile, StringComparison.OrdinalIgnoreCase)) return false;

            string lastDate = ExtractDate(lastFile);
            // Empêche de revenir en arrière dans le temps pour un même type de fichier
            if (lastDate != null && string.Compare(fileDate, lastDate) < 0)
            {
                _logger.LogDebug($"      [IGNORE] {fileName} (Plus ancien que le dernier traité)");
                return false;
            }
        }

        // Filtre de date de début configuré
        if (!string.IsNullOrEmpty(_config.StartDateHourMin) && string.Compare(fileDate, _config.StartDateHourMin) < 0)
            return false;

        return true;
    }

    /// <summary>
    /// Met à jour le registre LastProcessed.txt avec une clé séparant préfixe et suffixe.
    /// </summary>
    private void UpdateLastProcessed(string fileName)
    {
        var prefixes = _config.PrefixesFichiers.Split(',').Select(p => p.Trim());
        var suffixes = _config.SuffixesFichiers.Split(',').Select(s => s.Trim());

        string currentPrefix = prefixes.FirstOrDefault(p => fileName.StartsWith(p, StringComparison.OrdinalIgnoreCase));
        string currentSuffix = suffixes.FirstOrDefault(s => fileName.EndsWith(s, StringComparison.OrdinalIgnoreCase));

        if (currentPrefix != null && currentSuffix != null)
        {
            string trackingKey = $"{currentPrefix}|{currentSuffix}";
            _lastProcessedTracker[trackingKey] = fileName;

            // Utilisation d'un séparateur robuste pour la sérialisation
            var lines = _lastProcessedTracker.Select(kvp => $"{kvp.Key}#|#{kvp.Value}");
            File.WriteAllLines(_config.LastProcessedPath, lines);
        }
    }

    /// <summary>
    /// Charge l'historique des traitements depuis le fichier LastProcessed.txt au démarrage.
    /// </summary>
    private void LoadLastProcessed()
    {
        if (File.Exists(_config.LastProcessedPath))
        {
            var lines = File.ReadAllLines(_config.LastProcessedPath);
            foreach (var line in lines)
            {
                var parts = line.Split(new[] { "#|#" }, StringSplitOptions.None);
                if (parts.Length == 2) _lastProcessedTracker[parts[0]] = parts[1];
            }
        }
    }

    /// <summary>
    /// Extrait la date (14 caractères) du nom du fichier selon le format MSC standard.
    /// </summary>
    private static string ExtractDate(string f)
    {
        var p = f.Split('_');
        return p.Length > 1 && p[1].Length >= 14 ? p[1][..14] : null;
    }

    /// <summary>
    /// Décompression GZip.
    /// </summary>
    private static void Decompress(string gz, string txt)
    {
        using var fs = File.OpenRead(gz);
        using var gzst = new GZipStream(fs, CompressionMode.Decompress);
        using var outFs = File.Create(txt);
        gzst.CopyTo(outFs);
    }

    /// <summary>
    /// Compression GZip (niveau optimal).
    /// </summary>
    private static void Compress(string txt, string gz)
    {
        using var fs = File.Create(gz);
        using var gzst = new GZipStream(fs, CompressionLevel.Optimal);
        using var inFs = File.OpenRead(txt);
        inFs.CopyTo(gzst);
    }

    /// <summary>
    /// Normalisation des numéros de téléphone (ajout/retrait préfixe Togo).
    /// </summary>
    private static string Normalize(string n) => n.StartsWith("00228") ? "228" + n[5..] : (n.StartsWith("00") ? n[2..] : (n.Length == 8 ? "228" + n : n));

    /// <summary>
    /// Filtre spécifique pour msOriginating (Voix).
    /// </summary>
    private async Task<bool> ProcessMsOriginating(string txt) => await FilterGeneric(txt, 7, 63, "02");

    /// <summary>
    /// Filtre spécifique pour SMSinMSC (SMS).
    /// </summary>
    private async Task<bool> ProcessSmsMsc(string txt) => await FilterGeneric(txt, 31, 4, null);

    /// <summary>
    /// Logique de filtrage générique : lit le fichier ligne par ligne, normalise et applique le filtre B ou une valeur fixe.
    /// </summary>
    private async Task<bool> FilterGeneric(string txt, int numIdx, int filterIdx, string filterVal)
    {
        bool ok = false;
        string tmp = txt + ".tmp";

        using (var r = new StreamReader(txt, Encoding.UTF8))
        using (var w = new StreamWriter(tmp, false, Encoding.UTF8))
        {
            string header = await r.ReadLineAsync();
            if (header != null) await w.WriteLineAsync(header);

            while (!r.EndOfStream)
            {
                var line = await r.ReadLineAsync();
                if (string.IsNullOrWhiteSpace(line)) continue;
                var cols = line.Split('|');
                if (cols.Length <= Math.Max(numIdx, filterIdx)) continue;

                cols[numIdx] = Normalize(cols[numIdx]);
                // Si filterVal est null, on check dans le HashSet FileB, sinon on compare à la valeur fixe.
                bool isTarget = (filterVal == null) ? _fileB.Contains(cols[filterIdx]) : cols[filterIdx] == filterVal;

                if (isTarget)
                {
                    await w.WriteLineAsync(string.Join('|', cols));
                    ok = true;
                }
            }
        }

        if (ok) { File.Delete(txt); File.Move(tmp, txt); }
        else { if (File.Exists(tmp)) File.Delete(tmp); }
        return ok;
    }

    /// <summary>
    /// Gère l'envoi SFTP avec un mécanisme de tentative (retry) en cas d'échec.
    /// </summary>
    private void HandleFtpSendingWithRetry(string file, string dayKey)
    {
        int tries = 0; bool sent = false;
        string year = dayKey[..4], month = dayKey.Substring(4, 2), day = dayKey.Substring(6, 2);
        string remotePath = Path.Combine(_config.SftpSettings.UploadPath, "VOICE_SMS", year, month, day);

        while (tries++ < _config.FtpRetryCount && !sent)
        {
            sent = _ftp.SendFile(file, remotePath);
            if (!sent) Thread.Sleep(_config.FtpRetryDelayMs);
        }
    }
}