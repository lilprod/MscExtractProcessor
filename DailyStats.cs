using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;


    public class DailyStats
    {
        public int Detected { get; set; }   // Fichiers détectés
        public int Processed { get; set; }   // Fichiers traités avec succès
        public int Deleted { get; set; }     // Fichiers supprimés (non éligibles)
        public int FtpSuccess { get; set; }  // Envois FTP réussis
        public int FtpFailed { get; set; }   // Envois FTP échoués

        // Nouvelle statistique pour le comptage des erreurs de compression
        public int FailedCompression { get; set; }  // Nombre de compressions échouées

        // Nouvelle statistique pour le comptage des erreurs de décompression
        public int FailedDecompression { get; set; }  // Nombre de décompressions échouées
    }

