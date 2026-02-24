using Microsoft.Extensions.Options;
using Renci.SshNet;
using System;
using System.IO;

public class FtpHelper
{
    private readonly SftpSettings _sftpSettings;

    // Le constructeur prend maintenant SftpSettings via l'injection de dépendances
    public FtpHelper(IOptions<SftpSettings> sftpSettings)
    {
        _sftpSettings = sftpSettings.Value;
    }

    public bool SendFile(string filePath, string remotePath)
    {
        try
        {
            using (var sftp = new SftpClient(_sftpSettings.Host, _sftpSettings.Port, _sftpSettings.Username, _sftpSettings.Password))
            {
                sftp.Connect();

                // Crée les répertoires distants nécessaires
                CreateRemoteDirectoryStructure(sftp, remotePath);

                // Transfert du fichier
                using (var fileStream = new FileStream(filePath, FileMode.Open))
                {
                    sftp.UploadFile(fileStream, Path.Combine(remotePath, Path.GetFileName(filePath)));
                }

                sftp.Disconnect();
            }

            return true;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Erreur FTP lors de l'envoi du fichier {filePath} : {ex.Message}");
            return false;
        }
    }

    // Crée les répertoires distants dans SFTP si nécessaire
    private void CreateRemoteDirectoryStructure(SftpClient sftp, string remotePath)
    {
        var directories = remotePath.Split(new[] { '/' }, StringSplitOptions.RemoveEmptyEntries);
        string currentPath = string.Empty;

        foreach (var directory in directories)
        {
            currentPath = Path.Combine(currentPath, directory);

            if (!sftp.Exists(currentPath))
            {
                sftp.CreateDirectory(currentPath);
                Console.WriteLine($"Répertoire créé : {currentPath}");
            }
        }
    }
}
