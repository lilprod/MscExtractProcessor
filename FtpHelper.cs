using Microsoft.Extensions.Options;
using Microsoft.Extensions.Logging;
using Renci.SshNet;
using System;
using System.IO;

public class FtpHelper
{
    private readonly SftpSettings _sftpSettings;
    private readonly ILogger<FtpHelper> _logger; // Ajout du logger

    // Le constructeur prend maintenant SftpSettings via l'injection de dépendances
    // Le constructeur reçoit maintenant le logger via l'injection
    public FtpHelper(IOptions<SftpSettings> sftpSettings, ILogger<FtpHelper> logger)
    {
        _sftpSettings = sftpSettings.Value;
        _logger = logger;
    }

    public bool SendFile(string filePath, string remotePath)
    {
        try
        {
            using var sftp = new SftpClient(_sftpSettings.Host, _sftpSettings.Port, _sftpSettings.Username, _sftpSettings.Password);
            sftp.Connect();

            // 1. On s'assure que le chemin utilise des "/" pour Linux
            string linuxPath = remotePath.Replace("\\", "/");

            // 2. Création des dossiers
            CreateRemoteDirectoryStructure(sftp, linuxPath);

            // 3. Envoi du fichier
            using var fileStream = new FileStream(filePath, FileMode.Open);
            string fileName = Path.GetFileName(filePath);

            // On combine le chemin et le nom du fichier proprement
            string fullRemotePath = $"{linuxPath}/{fileName}".Replace("//", "/");

            sftp.UploadFile(fileStream, fullRemotePath);
            sftp.Disconnect();

            return true;
        }
        catch (Exception ex)
        {
            // Si le logger est null par accident, on utilise la Console pour ne pas perdre l'info
            if (_logger != null)
                _logger.LogError($"Échec SFTP : {ex.Message}");
            else
                Console.WriteLine($"Échec SFTP (Logger NULL) : {ex.Message}");

            return false;
        }
    }

    // Crée les répertoires distants dans SFTP si nécessaire
    private void CreateRemoteDirectoryStructure(SftpClient sftp, string remotePath)
    {
        // On nettoie le chemin pour avoir des "/" partout
        string path = remotePath.Replace("\\", "/");
        var parts = path.Split(new[] { '/' }, StringSplitOptions.RemoveEmptyEntries);

        string currentPath = "";

        foreach (var part in parts)
        {
            currentPath += "/" + part;

            try
            {
                if (!sftp.Exists(currentPath))
                {
                    sftp.CreateDirectory(currentPath);
                    _logger.LogInformation($"Répertoire créé sur SFTP : {currentPath}");
                }
            }
            catch (Exception)
            {
                // On log en "Debug" car c'est normal d'échouer sur les parents (ex: /mnt)
                // ou si le dossier vient d'être créé par un autre thread.
                _logger.LogDebug($"Info: Le dossier {currentPath} n'a pas été créé (existe déjà ou accès restreint).");
            }
        }
    }
}
