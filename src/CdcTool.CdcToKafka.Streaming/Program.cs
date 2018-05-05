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
                .AddEnvironmentVariables("CdcStreamer_"); // all environment variables with this prefix;

            IConfigurationRoot configuration = builder.Build();

            // get parameters and start
            var isFullLoad = IsFullLoad(args, configuration);
            var tables = GetTables(args, configuration);
            var interval = GetInterval(args, configuration);
            var serializationMode = GetSerializationMode(args, configuration);
            var sendWithKey = GetSendWithKey(args, configuration);
            var batchSize = GetBatchSize(args, configuration);
            var printMod = GetPrintMod(args, configuration);
            var cts = new CancellationTokenSource();

            if(isFullLoad)
            {
                var fullLoadStreamer = new FullLoadStreamer(configuration);
                fullLoadStreamer.StreamTablesAsync(cts.Token, tables, serializationMode, sendWithKey, batchSize, printMod).Wait();
                Console.WriteLine("Streaming to Kafka in progress. Press X to shutdown");

                // wait for shutdown signal
                bool shutdown = false;
                while (!shutdown)
                {
                    if (starting.IsSet)
                        shutdown = true;
                    else if (Console.ReadKey(true).Key == ConsoleKey.X)
                        shutdown = true;
                    else
                        Task.Delay(1000);
                }

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
                bool shutdown = false;
                while (!shutdown)
                {
                    if (starting.IsSet)
                        shutdown = true;
                    else if (Console.ReadKey(true).Key == ConsoleKey.X)
                        shutdown = true;
                    else
                        Task.Delay(1000);
                }

                Console.WriteLine("Received signal gracefully shutting down");
                cts.Cancel();
                cdcStreamer.WaitForCompletion();
                ended.Set();
            }
        }

        private static bool IsFullLoad(string[] args, IConfiguration configuration)
        {
            foreach (var arg in args)
            {
                if (arg.Equals("--fullload", StringComparison.OrdinalIgnoreCase) || arg.Equals("--f", StringComparison.OrdinalIgnoreCase))
                    return true;
            }

            var mode = configuration["mode"];
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
            for (int i = 0; i < args.Length; i++)
            {
                if (args[i].Equals("--table") || args[i].Equals("-t"))
                {
                    return args[i + 1].Replace("(", "").Replace(")", "").Split(',').ToList();
                }
            }

            if (configuration["tables"] != null)
            {
                return configuration["tables"].Split(',').ToList();
            }
            else
            {
                return new List<string>();
            }
        }

        private static TimeSpan GetInterval(string[] args, IConfiguration configuration)
        {
            for (int i = 0; i < args.Length; i++)
            {
                if (args[i].Equals("--interval") || args[i].Equals("-i"))
                {
                    return TimeSpan.FromMilliseconds(int.Parse(args[i + 1]));
                }
            }


            return TimeSpan.FromMilliseconds(int.Parse(configuration["intervalMs"]));
        }

        private static SerializationMode GetSerializationMode(string[] args, IConfiguration configuration)
        {
            for (int i = 0; i < args.Length; i++)
            {
                if (args[i].Equals("--serialization") || args[i].Equals("-s"))
                {
                    return (SerializationMode)Enum.Parse(typeof(SerializationMode), args[i + 1]);
                }
            }


            return (SerializationMode)Enum.Parse(typeof(SerializationMode), configuration["serializationMode"]);
        }

        private static int GetBatchSize(string[] args, IConfiguration configuration)
        {
            for (int i = 0; i < args.Length; i++)
            {
                if (args[i].Equals("--batchsize") || args[i].Equals("-b"))
                {
                    return int.Parse(args[i + 1]);
                }
            }

            return int.Parse(configuration["batchSize"]);
        }

        private static bool GetSendWithKey(string[] args, IConfiguration configuration)
        {
            for (int i = 0; i < args.Length; i++)
            {
                if (args[i].Equals("--sendkey") || args[i].Equals("-k"))
                {
                    return bool.Parse(args[i + 1]);
                }
            }

            return bool.Parse(configuration["sendWithKey"]);
        }

        private static int GetPrintMod(string[] args, IConfiguration configuration)
        {
            for (int i = 0; i < args.Length; i++)
            {
                if (args[i].Equals("--printmod") || args[i].Equals("-p"))
                {
                    return int.Parse(args[i + 1]);
                }
            }

            return int.Parse(configuration["printPercentProgressMod"]);
        }
    }
}
