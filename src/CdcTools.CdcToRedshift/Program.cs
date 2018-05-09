using CdcTools.CdcReader;
using CdcTools.CdcReader.Transactional;
using CdcTools.CdcToRedshift.NonTransactional;
using CdcTools.CdcToRedshift.Transactional;
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
            var runMode = GetRunMode(configuration);
            var tables = GetTables(configuration);
            var batchSize = GetNonTransactionalTableBatchSize(configuration);
            var redshiftClient = GetRedshiftClient(configuration);

            var cts = new CancellationTokenSource();

            if (runMode == RunMode.FullLoad)
            {
                var cdcReaderClient = new CdcReaderClient(configuration["DatabaseConnection"], configuration["StateManagmentConnection"]);
                var printMod = GetPrintMod(configuration);
                var fullLoadExporter = new FullLoadExporter(cdcReaderClient, redshiftClient);
                fullLoadExporter.ExportTablesAsync(cts.Token, executionId, tables, batchSize, printMod).Wait();
                Console.WriteLine("Export to Redshift started.");

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
            else if(runMode == RunMode.NonTransactionalCdc)
            {
                var interval = GetInterval(configuration);
                var cdcReaderClient = new CdcReaderClient(configuration["DatabaseConnection"], configuration["StateManagmentConnection"]);
                var cdcExporter = new ChangeExporter(cdcReaderClient, redshiftClient);
                cdcExporter.StartExportingChangesAsync(cts.Token, executionId, tables, interval, batchSize).Wait();
                Console.WriteLine("Export to Redshift in progress.");

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
            else
            {
                var interval = GetInterval(configuration);
                var cdcReaderClient = new CdcTransactionClient(configuration["DatabaseConnection"], configuration["StateManagmentConnection"]);
                var cdcExporter = new TransactionExporter(cdcReaderClient, redshiftClient);
                var perTableBufferLimit = GetPerTableBufferLimit(configuration);
                var tranBufferLimit = GetTransactionBufferLimit(configuration);
                var tranBatchSizeLimit = GetTransactionBatchSizeLimit(configuration);

                cdcExporter.StartExportingChangesAsync(cts.Token, executionId, tables, interval, perTableBufferLimit, tranBufferLimit, tranBatchSizeLimit).Wait();
                Console.WriteLine("Export to Redshift in progress.");

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

        private static RunMode GetRunMode(IConfiguration configuration)
        {
            var mode = configuration["Mode"];
            if (mode != null)
            {
                if (mode.Equals("cdc-nontran"))
                    return RunMode.NonTransactionalCdc;
                else if (mode.Equals("cdc-tran"))
                    return RunMode.TransactionalCdc;
                else if (mode.Equals("full-load"))
                    return RunMode.FullLoad;
            }

            return RunMode.NonTransactionalCdc;
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

        private static int GetNonTransactionalTableBatchSize(IConfiguration configuration)
        {
            return int.Parse(configuration["NonTransactionalTableBatchSize"]);
        }

        private static int GetPerTableBufferLimit(IConfiguration configuration)
        {
            return int.Parse(configuration["PerTableBufferLimit"]);
        }

        private static int GetTransactionBufferLimit(IConfiguration configuration)
        {
            return int.Parse(configuration["TransactionBufferLimit"]);
        }

        private static int GetTransactionBatchSizeLimit(IConfiguration configuration)
        {
            return int.Parse(configuration["TransactionBatchSizeLimit"]);
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
