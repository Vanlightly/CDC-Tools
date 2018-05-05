using CdcTools.KafkaToRedshift.Consumers;
using CdcTools.KafkaToRedshift.Redshift;
using CdcTools.Redshift;
using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.Loader;
using System.Threading;
using System.Threading.Tasks;

namespace CdcTools.KafkaToRedshift
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.Title = "Kafka to Redshift Writer";

            var ended = new ManualResetEventSlim();
            var starting = new ManualResetEventSlim();

            AssemblyLoadContext.Default.Unloading += ctx =>
            {
                System.Console.WriteLine("Unloading fired");
                starting.Set();
                System.Console.WriteLine("Waiting for completion");
                ended.Wait();
            };

            var builder = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true)
                .AddEnvironmentVariables("CDCTOOLS_"); // all environment variables with this prefix;

            IConfigurationRoot configuration = builder.Build();

            var tables = GetTables(args, configuration);
            var windowSizePeriod = GetWindowSizeTimePeriod(args, configuration);
            var windowSizeItems = GetWindowSizeItemCount(args, configuration);
            var serializationMode = GetSerializationMode(args, configuration);
            var messagesHaveKey = MessagesHaveKey(args, configuration);

            var kafkaSources = tables.Select(x => new KafkaSource()
            {
                Table = x,
                Topic = configuration["tableTopicPrefix"] + x.ToLower()
            }).ToList();

            var cts = new CancellationTokenSource();

            IConsumer consumer = GetConsumer(serializationMode, messagesHaveKey, configuration);
            consumer.StartConsumingAsync(cts.Token, windowSizePeriod, windowSizeItems, kafkaSources).Wait();
            Console.WriteLine($"Consuming messages of tables {string.Join(',', tables)} in {serializationMode.ToString()} deserialization mode with {windowSizePeriod} window sizes");
            Console.WriteLine("Press X to shutdown");

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
            consumer.WaitForCompletion();
            ended.Set();
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

        private static TimeSpan GetWindowSizeTimePeriod(string[] args, IConfiguration configuration)
        {
            for (int i = 0; i < args.Length; i++)
            {
                if (args[i].Equals("--window-ms") || args[i].Equals("-wm"))
                {
                    return TimeSpan.FromMilliseconds(int.Parse(args[i + 1]));
                }
            }

            return TimeSpan.FromMilliseconds(int.Parse(configuration["WindowMs"]));
        }

        private static int GetWindowSizeItemCount(string[] args, IConfiguration configuration)
        {
            for (int i = 0; i < args.Length; i++)
            {
                if (args[i].Equals("--window-items") || args[i].Equals("-wi"))
                {
                    return int.Parse(args[i + 1]);
                }
            }


            return int.Parse(configuration["WindowItems"]);
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

        private static bool MessagesHaveKey(string[] args, IConfiguration configuration)
        {
            for (int i = 0; i < args.Length; i++)
            {
                if (args[i].Equals("--messagekey") || args[i].Equals("-k"))
                {
                    return bool.Parse(args[i + 1]);
                }
            }

            return bool.Parse(configuration["messagesHaveKey"]);
        }

        private static IRedshiftWriter GetRedshiftWriter(IConfiguration configuration)
        {
            return new RedshiftWriter(new RedshiftClient(new RedshiftConfiguration()
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
            }));
        }

        private static IConsumer GetConsumer(SerializationMode serializationMode, bool messagesHaveKey, IConfiguration configuration)
        {
            var redshiftWriter = GetRedshiftWriter(configuration);
            if (serializationMode == SerializationMode.Avro)
            {
                if (messagesHaveKey)
                    return new KeyedAvroConsumer(redshiftWriter);
                else
                    return new NonKeyedAvroConsumer(redshiftWriter);
            }
            else
            {
                if (messagesHaveKey)
                    return new KeyedJsonConsumer(redshiftWriter);
                else
                    return new NonKeyedJsonConsumer(redshiftWriter);
            }
        }
    }
}
