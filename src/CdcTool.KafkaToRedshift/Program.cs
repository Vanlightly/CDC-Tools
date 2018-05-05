using CdcTools.KafkaToRedshift.Consumers;
using CdcTools.KafkaToRedshift.Redshift;
using CdcTools.Redshift;
using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;

namespace CdcTools.KafkaToRedshift
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.Title = "Kafka to Redshift Writer";

            var builder = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true)
                .AddEnvironmentVariables("CDCTOOLS_"); // all environment variables with this prefix;

            IConfigurationRoot configuration = builder.Build();

            var tables = GetTables(args, configuration);
            var interval = GetInterval(args, configuration);
            var serializationMode = GetSerializationMode(args, configuration);
            var messagesHaveKey = MessagesHaveKey(args, configuration);

            var kafkaSources = tables.Select(x => new KafkaSource()
            {
                Table = x,
                Topic = configuration["tableTopicPrefix"] + x.ToLower()
            }).ToList();

            var cts = new CancellationTokenSource();

            IConsumer consumer = GetConsumer(serializationMode, messagesHaveKey, configuration);
            consumer.StartConsumingAsync(cts.Token, interval, kafkaSources).Wait();
            Console.WriteLine($"Consuming messages of tables {string.Join(',', tables)} in {serializationMode.ToString()} deserialization mode with {interval} window sizes");
            Console.WriteLine("Press any key to shutdoown");
            Console.ReadKey();
            cts.Cancel();
            consumer.WaitForCompletion();
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
