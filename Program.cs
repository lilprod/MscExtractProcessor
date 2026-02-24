using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.DependencyInjection; // Déjà présent normalement
using Microsoft.Extensions.Options;            // Optionnel mais utile
using Microsoft.Extensions.Configuration;      // Pour IConfiguration
using Serilog;

internal class Program
{
    public static void Main(string[] args)
    {
        // Initialisation du Logger avant tout pour capturer les erreurs de config
        Log.Logger = new LoggerConfiguration()
            .WriteTo.Console()
            .CreateBootstrapLogger();

        try
        {
            Log.Information("=== INITIALISATION DU SERVICE MSC EXTRACT ===");

            var host = Host.CreateDefaultBuilder(args)
                .UseSerilog((context, services, loggerConfiguration) =>
                {
                    // On utilise context.Configuration pour accéder aux paramètres du appsettings.json
                    string logFolder = context.Configuration["ExtractConfig:LogDirectory"];

                    if (string.IsNullOrWhiteSpace(logFolder))
                        logFolder = Path.Combine(AppContext.BaseDirectory, "Logs");

                    Directory.CreateDirectory(logFolder);

                    loggerConfiguration
                        .ReadFrom.Configuration(context.Configuration) // Charge les niveaux de log depuis le JSON
                        .Enrich.FromLogContext()
                        .WriteTo.Console()
                        .WriteTo.File(
                            Path.Combine(logFolder, "MscExtractProcessor_.log"),
                            rollingInterval: RollingInterval.Day,
                            outputTemplate: "[{Timestamp:yyyy-MM-dd HH:mm:ss} {Level:u3}] {Message:lj}{NewLine}{Exception}");
                })
                .ConfigureServices((hostContext, services) =>
                {
                    // Injection des configurations
                    services.Configure<SftpSettings>(hostContext.Configuration.GetSection("ExtractConfig:SftpSettings"));
                    services.Configure<ExtractConfig>(hostContext.Configuration.GetSection("ExtractConfig"));

                    // Services Applicatifs
                    services.AddSingleton<FtpHelper>();
                    services.AddSingleton<FileProcessor>();
                    services.AddHostedService<Worker>();
                })
                .Build();

            host.Run();
        }
        catch (Exception ex)
        {
            Log.Fatal(ex, "Le service s'est arrêté de manière inattendue.");
        }
        finally
        {
            Log.CloseAndFlush();
        }
    }
}