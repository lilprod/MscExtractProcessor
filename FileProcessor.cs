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
/// Gère le filtrage, la décompression, le traitement métier et l'envoi FTP des fichiers MSC.
/// </summary>
public class FileProcessor
{
    private readonly ExtractConfig _config;
    private readonly ILogger<FileProcessor> _logger;
    private readonly FtpHelper _ftp;
    private readonly HashSet<string> _fileB;

    // Statistiques journalières : Clé = YYYYMMDD
    private readonly Dictionary<string, DailyStats> _stats = new();

    // Suivi indépendant du dernier fichier traité par préfixe (LOMBC1_, LOMBC2_)
    private readonly ConcurrentDictionary<string, string> _lastProcessedByPrefix = new();

    // Verrous par fichier pour éviter les accès concurrents entre Scan et Watcher
    private readonly ConcurrentDictionary<string, SemaphoreSlim> _fileLocks = new();

    public ExtractConfig Config => _config;

    public FileProcessor(IOptions<ExtractConfig> config, ILogger<FileProcessor> logger, FtpHelper ftp)
    {
        _config = config.Value;
        _logger = logger;
        _ftp = ftp;

        // Initialisation de l'environnement
        Directory.CreateDirectory(Path.GetDirectoryName(_config.LastProcessedPath));
        LoadLastProcessed();

        // Chargement de la liste de filtrage B (HashSet pour recherche O(1))
        if (File.Exists(_config.FileBPath))
        {
            _fileB = new HashSet<string>(File.ReadAllLines(_config.FileBPath));
        }
        else
        {
            _logger.LogWarning($"Fichier de filtrage absent : {_config.FileBPath}");
            _fileB = new HashSet<string>();
        }
    }

    /// <summary>
    /// Scanne les dossiers existants de manière séquentielle lors du démarrage.
    /// </summary>
    public async Task ProcessInitialFoldersSequentiallyAsync(ConcurrentDictionary<string, byte> processedFiles, CancellationToken ct)
    {
        string currentYear = DateTime.Now.ToString("yyyy");
        string folderYear = _config.StartDateHourMin.Length >= 4 ? _config.StartDateHourMin[..4] : currentYear;
        string folderToScan = _config.StartDateHourMin.Length >= 6 ? _config.StartDateHourMin.Substring(4, 2) : DateTime.Now.ToString("MM");

        string rootPath = Path.Combine(_config.DossierSource, folderYear, folderToScan);

        if (!Directory.Exists(rootPath))
        {
            _logger.LogError($"Répertoire racine introuvable : {rootPath}");
            return;
        }

        var dayDirectories = Directory.EnumerateDirectories(rootPath).OrderBy(d => d);

        foreach (var dayDir in dayDirectories)
        {
            if (ct.IsCancellationRequested) break;

            string dayName = Path.GetFileName(dayDir);
            _logger.LogInformation($"---> [PHASE SCAN] Dossier jour : {dayName}");

            var files = Directory.EnumerateFiles(dayDir, "*.gz").OrderBy(f => f).ToList();
            int count = 0;

            foreach (var file in files)
            {
                if (IsFileEligible(file))
                {
                    _logger.LogInformation($"      [OK] Détecté : {Path.GetFileName(file)}");
                    await ProcessFileIfEligibleAsync(file);
                    processedFiles.TryAdd(file, 0);
                    count++;
                }
            }

            if (count > 0)
                _logger.LogInformation($"---> [FIN JOURNÉE] {dayName} terminé ({count} fichiers traités).");
        }
    }

    /// <summary>
    /// Point d'entrée sécurisé pour traiter un fichier (vérifie l'éligibilité et verrouille l'accès).
    /// </summary>
    public async Task ProcessFileIfEligibleAsync(string filePath)
    {
        if (!IsFileEligible(filePath)) return;

        var fileLock = _fileLocks.GetOrAdd(filePath, new SemaphoreSlim(1, 1));
        await fileLock.WaitAsync();

        try
        {
            string fileName = Path.GetFileName(filePath);
            string date = ExtractDate(fileName);

            _logger.LogInformation($"      [TRAITEMENT EN COURS] {fileName}");
            await ProcessFileAsync(filePath, fileName, date);

            // Persistance de l'état par préfixe après succès
            UpdateLastProcessed(fileName);
        }
        finally
        {
            fileLock.Release();
        }
    }

    private async Task ProcessFileAsync(string path, string fileName, string date)
    {
        string year = date.Substring(0, 4);
        string month = date.Substring(4, 2);
        string day = date.Substring(6, 2);
        string key = year + month + day;

        // Gestion des DailyStats
        if (!_stats.ContainsKey(key)) _stats[key] = new DailyStats();
        _stats[key].Detected++;

        // Création d'un espace de travail temporaire unique
        string tempDir = Path.Combine(Path.GetTempPath(), "MscExtract_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(tempDir);

        try
        {
            string txtFile = Path.Combine(tempDir, fileName.Replace(".gz", ""));
            Decompress(path, txtFile);

            // Routage vers le filtre spécifique
            bool isMatch = fileName.Contains("_msOriginating")
                ? await ProcessMsOriginating(txtFile)
                : await ProcessSmsMsc(txtFile);

            if (!isMatch)
            {
                _stats[key].Deleted++;
                return;
            }

            _stats[key].Processed++;

            // Arborescence de sortie : Année > Mois > Jour
            string outDir = Path.Combine(_config.OutputDirectory, year, month, day);
            Directory.CreateDirectory(outDir);

            string finalPath = Path.Combine(outDir, fileName);
            Compress(txtFile, finalPath);

            // Envoi SFTP si configuré
            if (_config.SendFileFTP.Equals("oui", StringComparison.OrdinalIgnoreCase))
            {
                HandleFtpSendingWithRetry(finalPath, key);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError($"Erreur lors du traitement de {fileName} : {ex.Message}");
        }
        finally
        {
            // Nettoyage du dossier temporaire sécurisé par try-catch
            try
            {
                if (Directory.Exists(tempDir)) Directory.Delete(tempDir, true);
            }
            catch (Exception ex)
            {
                _logger.LogWarning($"Nettoyage différé pour {tempDir} : {ex.Message}");
            }
        }
    }

    /// <summary>
    /// Logique de filtrage robuste : gère les doublons temporels et l'indépendance des préfixes.
    /// </summary>
    private bool IsFileEligible(string filePath)
    {
        string fileName = Path.GetFileName(filePath);
        string fileDate = ExtractDate(fileName);
        if (fileDate == null) return false;

        // 1. Validation Préfixe
        var prefixes = _config.PrefixesFichiers.Split(',').Select(p => p.Trim()).ToList();
        string currentPrefix = prefixes.FirstOrDefault(p => fileName.StartsWith(p, StringComparison.OrdinalIgnoreCase));
        if (currentPrefix == null) return false;

        // 2. Validation Suffixe
        var suffixes = _config.SuffixesFichiers.Split(',').Select(s => s.Trim()).ToList();
        bool hasSuffix = suffixes.Any(s => fileName.EndsWith(s.Trim(), StringComparison.OrdinalIgnoreCase));

        if (!File.Exists(filePath) || new FileInfo(filePath).Length == 0 || !hasSuffix) return false;

        // 3. Comparaison avec le dernier traité pour ce flux spécifique
        if (_lastProcessedByPrefix.TryGetValue(currentPrefix, out string lastFile))
        {
            // Ignorer uniquement si le nom est strictement identique (doublon FileSystemWatcher)
            if (fileName.Equals(lastFile, StringComparison.OrdinalIgnoreCase))
            {
                _logger.LogDebug($"      [IGNORE] {fileName} (Doublon déjà traité)");
                return false;
            }

            // Comparaison des dates pour éviter de traiter des fichiers plus anciens après un redémarrage
            string lastDate = ExtractDate(lastFile);
            if (lastDate != null && string.Compare(fileDate, lastDate) < 0)
            {
                _logger.LogInformation($"      [IGNORE] {fileName} (Date antérieure au dernier fichier traité : {lastFile})");
                return false;
            }

            // Note : Si les dates sont identiques mais les noms différents, on laisse passer (ex: msOriginating vs SMSinMSC)
        }

        // 4. Filtre de date de démarrage (StartDateHourMin)
        if (!string.IsNullOrEmpty(_config.StartDateHourMin) && string.Compare(fileDate, _config.StartDateHourMin) < 0)
        {
            return false;
        }

        return true;
    }

    private void UpdateLastProcessed(string fileName)
    {
        var prefixes = _config.PrefixesFichiers.Split(',').Select(p => p.Trim());
        string currentPrefix = prefixes.FirstOrDefault(p => fileName.StartsWith(p, StringComparison.OrdinalIgnoreCase));

        if (currentPrefix != null)
        {
            _lastProcessedByPrefix[currentPrefix] = fileName;

            // Sauvegarde multi-ligne : PREFIXE|NOM_FICHIER
            var lines = _lastProcessedByPrefix.Select(kvp => $"{kvp.Key}|{kvp.Value}");
            File.WriteAllLines(_config.LastProcessedPath, lines);
        }
    }

    private void LoadLastProcessed()
    {
        if (File.Exists(_config.LastProcessedPath))
        {
            var lines = File.ReadAllLines(_config.LastProcessedPath);
            foreach (var line in lines)
            {
                var parts = line.Split('|');
                if (parts.Length == 2) _lastProcessedByPrefix[parts[0]] = parts[1];
            }
        }
    }

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

    private static string Normalize(string n) => n.StartsWith("00228") ? "228" + n[5..] : (n.StartsWith("00") ? n[2..] : (n.Length == 8 ? "228" + n : n));

    private static string ExtractDate(string f)
    {
        var p = f.Split('_');
        return p.Length > 1 && p[1].Length >= 14 ? p[1][..14] : null;
    }

    private static void Decompress(string gz, string txt)
    {
        using var fs = File.OpenRead(gz);
        using var gzst = new GZipStream(fs, CompressionMode.Decompress);
        using var outFs = File.Create(txt);
        gzst.CopyTo(outFs);
    }

    private static void Compress(string txt, string gz)
    {
        using var fs = File.Create(gz);
        using var gzst = new GZipStream(fs, CompressionLevel.Optimal);
        using var inFs = File.OpenRead(txt);
        inFs.CopyTo(gzst);
    }

    private async Task<bool> ProcessMsOriginating(string txt) => await FilterGeneric(txt, 7, 63, "02");
    private async Task<bool> ProcessSmsMsc(string txt) => await FilterGeneric(txt, 31, 4, null);
}