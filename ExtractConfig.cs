public class ExtractConfig
{
    // Dossier source des fichiers à traiter
    public string DossierSource { get; set; }

    // Dossier où les fichiers traités seront sauvegardés
    public string OutputDirectory { get; set; }

    // Dossier où les logs seront enregistrés
    public string LogDirectory { get; set; }

    // Chemin du fichier B utilisé pour le filtrage
    public string FileBPath { get; set; }

    // Chemin du fichier où est stocké le dernier fichier traité
    public string LastProcessedPath { get; set; }

    // Date et heure de début pour filtrer les fichiers (format: AAAAMMJJHHMM)
    public string StartDateHourMin { get; set; }

    // Date et heure de fin pour filtrer les fichiers (format: AAAAMMJJHHMM)
    public string EndDateHourMin { get; set; }

    // Si "oui", envoyer les fichiers traités via FTP, sinon "non"
    public string SendFileFTP { get; set; }

    // Nombre de tentatives de réessai FTP en cas d'échec
    public int FtpRetryCount { get; set; }

    // Délai en millisecondes entre chaque tentative de réessai FTP
    public int FtpRetryDelayMs { get; set; }

    // Paramètres de connexion SFTP
    public SftpSettings SftpSettings { get; set; }

    // Liste des préfixes valides pour les fichiers à traiter (ex: "LOMBC1_", "LOMBC2_")
    public string PrefixesFichiers { get; set; }

    // Liste des suffixes valides pour les fichiers à traiter (ex: "_msOriginating.txt.gz", "_mSOriginatingSMSinMSC.txt.gz")
    public string SuffixesFichiers { get; set; }

    // Dossier où les fichiers envoyés avec succès via FTP seront stockés
    public string SentFtpRecordsSuccess { get; set; }

    // Dossier où les fichiers échoués à l'envoi FTP seront stockés
    public string SentFtpRecordsFailed { get; set; }
}

public class SftpSettings
{
    // Hôte du serveur SFTP (ex: "10.80.16.223")
    public string Host { get; set; }

    // Port du serveur SFTP (par défaut 22)
    public int Port { get; set; }

    // Nom d'utilisateur pour se connecter au serveur SFTP
    public string Username { get; set; }

    // Mot de passe pour se connecter au serveur SFTP
    public string Password { get; set; }

    // Chemin d'upload sur le serveur SFTP (ex: "/mnt/disk/vol1/POSTPAID")
    public string UploadPath { get; set; }
}
