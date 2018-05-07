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
            var tables = GetTables(configuration);
            var windowSizePeriod = GetWindowSizeTimePeriod(configuration);
            var windowSizeItems = GetWindowSizeItemCount(configuration);
            var serializationMode = GetSerializationMode(configuration);
            var messagesHaveKey = MessagesHaveKey(configuration);
            
            var kafkaSources = tables.Select(x => new KafkaSource()
            {
                Table = x,
                Topic = configuration["TableTopicPrefix"] + x.ToLower()
            }).ToList();

            var cts = new CancellationTokenSource();

            IConsumer consumer = GetConsumer(serializationMode, messagesHaveKey, configuration);
            bool startedOk = consumer.StartConsumingAsync(cts.Token, windowSizePeriod, windowSizeItems, kafkaSources).Result;
            if (startedOk)
            {
                Console.WriteLine($"Consuming messages of tables {string.Join(',', tables)} in {serializationMode.ToString()} deserialization mode with {windowSizePeriod} window sizes");

#if DEBUG
                Console.WriteLine("Press any key to shutdown");
                Console.ReadKey();
#else
                starting.Wait();
                Console.WriteLine("Received signal gracefully shutting down");
#endif
            }
            else
            {
                Console.WriteLine("Failed to start up correctly, shutting down");
            }
            
            cts.Cancel();
            consumer.WaitForCompletion();
            ended.Set();
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

        private static TimeSpan GetWindowSizeTimePeriod(IConfiguration configuration)
        {
            return TimeSpan.FromMilliseconds(int.Parse(configuration["WindowMs"]));
        }

        private static int GetWindowSizeItemCount(IConfiguration configuration)
        {
            return int.Parse(configuration["WindowItems"]);
        }

        private static SerializationMode GetSerializationMode(IConfiguration configuration)
        {
            return (SerializationMode)Enum.Parse(typeof(SerializationMode), configuration["SerializationMode"]);
        }

        private static bool MessagesHaveKey(IConfiguration configuration)
        {
            return bool.Parse(configuration["messagesHaveKey"]);
        }

        private static string GetBootstrapServers(IConfiguration configuration)
        {
            return configuration["KafkaBootstrapServers"];
        }

        private static string GetSchemaRegistryUrl(IConfiguration configuration)
        {
            return configuration["KafkaSchemaRegistryUrl"];
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
                    return new KeyedAvroConsumer(redshiftWriter, GetBootstrapServers(configuration), GetSchemaRegistryUrl(configuration));
                else
                    return new NonKeyedAvroConsumer(redshiftWriter, GetBootstrapServers(configuration), GetSchemaRegistryUrl(configuration));
            }
            else
            {
                if (messagesHaveKey)
                    return new KeyedJsonConsumer(redshiftWriter, GetBootstrapServers(configuration));
                else
                    return new NonKeyedJsonConsumer(redshiftWriter, GetBootstrapServers(configuration));
            }
        }
    }
}
