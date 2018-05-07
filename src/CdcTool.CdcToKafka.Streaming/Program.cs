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
            var isFullLoad = IsFullLoad(args, configuration);
            var tables = GetTables(args, configuration);
            var interval = GetInterval(args, configuration);
            var serializationMode = GetSerializationMode(args, configuration);
            var sendWithKey = GetSendWithKey(args, configuration);
            var batchSize = GetBatchSize(args, configuration);
            var printMod = GetPrintMod(args, configuration);
            var kafkaBootstrapServers = GetBootstrapServers(args, configuration);
            var schemaRegistryUrl = GetSchemaRegistryUrl(args, configuration);
            var cts = new CancellationTokenSource();

            if(isFullLoad)
            {
                var fullLoadStreamer = new FullLoadStreamer(configuration);
                fullLoadStreamer.StreamTablesAsync(cts.Token, tables, serializationMode, sendWithKey, batchSize, printMod).Wait();
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
                fullLoadStreamer.WaitForCompletion();
                ended.Set();
            }
            else
            {
                var cdcRequest = new CdcRequest()
                {
                    BatchSize = batchSize,
                    Interval = interval,
                    SendWithKey = sendWithKey,
                    SerializationMode = serializationMode,
                    Tables = tables
                };
                var cdcStreamer = new ChangeStreamer(configuration);
                cdcStreamer.StartReading(cts.Token, cdcRequest);
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
                cdcStreamer.WaitForCompletion();
                ended.Set();
            }
        }

        private static bool IsFullLoad(string[] args, IConfiguration configuration)
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

        private static List<string> GetTables(string[] args, IConfiguration configuration)
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

        private static TimeSpan GetInterval(string[] args, IConfiguration configuration)
        {
            return TimeSpan.FromMilliseconds(int.Parse(configuration["IntervalMs"]));
        }

        private static SerializationMode GetSerializationMode(string[] args, IConfiguration configuration)
        {
            return (SerializationMode)Enum.Parse(typeof(SerializationMode), configuration["SerializationMode"]);
        }

        private static int GetBatchSize(string[] args, IConfiguration configuration)
        {
            return int.Parse(configuration["BatchSize"]);
        }

        private static bool GetSendWithKey(string[] args, IConfiguration configuration)
        {
            return bool.Parse(configuration["SendWithKey"]);
        }

        private static int GetPrintMod(string[] args, IConfiguration configuration)
        {
            return int.Parse(configuration["PrintPercentProgressMod"]);
        }

        private static string GetBootstrapServers(string[] args, IConfiguration configuration)
        {
            return configuration["KafkaBootstrapServers"];
        }

        private static string GetSchemaRegistryUrl(string[] args, IConfiguration configuration)
        {
            return configuration["KafkaSchemaRegistryUrl"];
        }
    }
}
