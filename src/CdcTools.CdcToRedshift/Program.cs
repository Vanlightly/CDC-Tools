using CdcTools.CdcReader;
using CdcTools.Redshift;
using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.Loader;
using System.Threading;

namespace CdcTools.CdcToRedshift
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.Title = "CDC to Redshift";

            // support graceful shutdown in Docker
            var ended = new ManualResetEventSlim();
            var starting = new ManualResetEventSlim();

            AssemblyLoadContext.Default.Unloading += ctx =>
            {
                System.Console.WriteLine("Unloading fired");
                starting.Set();
                System.Console.WriteLine("Waiting for completion");
                ended.Wait();
            };

            // set up configuration
            var builder = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true)
                .AddCommandLine(args)
                .AddEnvironmentVariables("CDCTOOLS_"); // all environment variables with this prefix;

            IConfigurationRoot configuration = builder.Build();

            var executionId = GetExecutionId(configuration);
            var isFullLoad = IsFullLoad(configuration);
            var tables = GetTables(configuration);
            var batchSize = GetBatchSize(configuration);
            var cdcReaderClient = new CdcReaderClient(configuration["DatabaseConnection"], configuration["StateManagmentConnection"]);
            var redshiftClient = GetRedshiftClient(configuration);

            var cts = new CancellationTokenSource();

            if (isFullLoad)
            {
                var printMod = GetPrintMod(configuration);
                var fullLoadExporter = new FullLoadExporter(cdcReaderClient, redshiftClient);
                fullLoadExporter.ExportTablesAsync(cts.Token, executionId, tables, batchSize, printMod).Wait();
                Console.WriteLine("Export started.");

                Thread.Sleep(2000);
                bool shutdown = false;
                // wait for shutdown signal
#if DEBUG
                Console.WriteLine("Press any key to shutdown");
                
                while (!shutdown)
                {
                    if (Console.KeyAvailable)
                        shutdown = true;
                    else if (fullLoadExporter.HasFinished())
                        shutdown = true;

                    Thread.Sleep(500);
                }
#else
                while (!shutdown)
                {
                    if (starting.IsSet)
                        shutdown = true;
                    else if (fullLoadExporter.HasFinished())
                        shutdown = true;

                    Thread.Sleep(500);
                }
#endif

                Console.WriteLine("Received signal gracefully shutting down");
                cts.Cancel();
                fullLoadExporter.WaitForCompletion();
                ended.Set();
            }
            else
            {
                var interval = GetInterval(configuration);
                var cdcExporter = new ChangeExporter(cdcReaderClient, redshiftClient);
                cdcExporter.StartExportingChangesAsync(cts.Token, executionId, tables, interval, batchSize).Wait();
                Console.WriteLine("Streaming to Kafka in progress. Press X to shutdown");

                // wait for shutdown signal
#if DEBUG
                Console.WriteLine("Press any key to shutdown");
                Console.ReadKey();
#else
                starting.Wait();
#endif

                Console.WriteLine("Received signal gracefully shutting down");
                cts.Cancel();
                cdcExporter.WaitForCompletion();
                ended.Set();
            }

        }

        private static string GetExecutionId(IConfiguration configuration)
        {
            if (configuration["ExecutionId"] == null)
                return Guid.NewGuid().ToString();

            return configuration["ExecutionId"];
        }

        private static bool IsFullLoad(IConfiguration configuration)
        {
            var mode = configuration["Mode"];
            if (mode != null)
            {
                if (mode.Equals("cdc"))
                    return false;
                else if (mode.Equals("full-load"))
                    return true;
            }

            return false;
        }

        private static List<string> GetTables(IConfiguration configuration)
        {
            if (configuration["Tables"] != null)
            {
                return configuration["Tables"].Split(',').ToList();
            }
            else
            {
                return new List<string>();
            }
        }

        private static TimeSpan GetInterval(IConfiguration configuration)
        {
            return TimeSpan.FromMilliseconds(int.Parse(configuration["IntervalMs"]));
        }

        private static int GetBatchSize(IConfiguration configuration)
        {
            return int.Parse(configuration["BatchSize"]);
        }

        private static int GetPrintMod(IConfiguration configuration)
        {
            return int.Parse(configuration["PrintPercentProgressMod"]);
        }

        private static RedshiftClient GetRedshiftClient(IConfiguration configuration)
        {
            return new RedshiftClient(new RedshiftConfiguration()
            {
                AccessKey = configuration["AccessKey"],
                SecretAccessKey = configuration["SecretAccessKey"],
                Region = configuration["AwsRegion"],
                Port = configuration["RedshiftPort"],
                Server = configuration["RedshiftServer"],
                MasterUsername = configuration["RedshiftUser"],
                MasterUserPassword = configuration["RedshiftPassword"],
                DBName = configuration["RedshiftDbName"],
                IamRole = configuration["RedshiftRole"],
                S3BucketName = configuration["S3BucketName"]
            });
        }
    }
}
