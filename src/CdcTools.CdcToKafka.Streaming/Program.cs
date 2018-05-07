using CdcTools.CdcReader;
using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.Loader;
using System.Threading;
using System.Threading.Tasks;

namespace CdcTools.CdcToKafka.Streaming
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.Title = "CDC To Kafka Streamer";

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

            // get parameters and start
            var executionId = GetExecutionId(configuration);
            var isFullLoad = IsFullLoad(configuration);
            var tables = GetTables(configuration);
            var serializationMode = GetSerializationMode(configuration);
            var sendWithKey = GetSendWithKey(configuration);
            var batchSize = GetBatchSize(configuration);
            var kafkaBootstrapServers = GetBootstrapServers(configuration);
            var schemaRegistryUrl = GetSchemaRegistryUrl(configuration);
            var cdcReaderClient = new CdcReaderClient(configuration["DatabaseConnection"], configuration["StateManagmentConnection"]);
            var cts = new CancellationTokenSource();

            if(isFullLoad)
            {
                var printMod = GetPrintMod(configuration);
                var fullLoadStreamer = new FullLoadStreamer(configuration, cdcReaderClient);
                fullLoadStreamer.StreamTablesAsync(cts.Token, executionId, tables, serializationMode, sendWithKey, batchSize, printMod).Wait();
                Console.WriteLine("Streaming to Kafka in progress.");

                Thread.Sleep(2000);
                bool shutdown = false;
                // wait for shutdown signal
#if DEBUG
                Console.WriteLine("Press any key to shutdown");

                while (!shutdown)
                {
                    if (Console.KeyAvailable)
                        shutdown = true;
                    else if (fullLoadStreamer.HasFinished())
                        shutdown = true;

                    Thread.Sleep(500);
                }
#else
                while (!shutdown)
                {
                    if (starting.IsSet)
                        shutdown = true;
                    else if (fullLoadStreamer.HasFinished())
                        shutdown = true;

                    Thread.Sleep(500);
                }
#endif

                Console.WriteLine("Received signal gracefully shutting down");
                cts.Cancel();
                fullLoadStreamer.WaitForCompletion();
                ended.Set();
            }
            else
            {
                var interval = GetInterval(configuration);

                var cdcRequest = new CdcRequest()
                {
                    BatchSize = batchSize,
                    ExecutionId = executionId,
                    Interval = interval,
                    SendWithKey = sendWithKey,
                    SerializationMode = serializationMode,
                    Tables = tables
                };
                var cdcStreamer = new ChangeStreamer(configuration, cdcReaderClient);
                cdcStreamer.StartReading(cts.Token, cdcRequest);
                Console.WriteLine("Streaming to Kafka started.");

                // wait for shutdown signal
#if DEBUG
                Console.WriteLine("Press any key to shutdown");
                Console.ReadKey();
#else
                starting.Wait();
#endif

                Console.WriteLine("Received signal gracefully shutting down");
                cts.Cancel();
                cdcStreamer.WaitForCompletion();
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

        private static SerializationMode GetSerializationMode(IConfiguration configuration)
        {
            return (SerializationMode)Enum.Parse(typeof(SerializationMode), configuration["SerializationMode"]);
        }

        private static int GetBatchSize(IConfiguration configuration)
        {
            return int.Parse(configuration["BatchSize"]);
        }

        private static bool GetSendWithKey(IConfiguration configuration)
        {
            return bool.Parse(configuration["SendWithKey"]);
        }

        private static int GetPrintMod(IConfiguration configuration)
        {
            return int.Parse(configuration["PrintPercentProgressMod"]);
        }

        private static string GetBootstrapServers(IConfiguration configuration)
        {
            return configuration["KafkaBootstrapServers"];
        }

        private static string GetSchemaRegistryUrl(IConfiguration configuration)
        {
            return configuration["KafkaSchemaRegistryUrl"];
        }
    }
}
