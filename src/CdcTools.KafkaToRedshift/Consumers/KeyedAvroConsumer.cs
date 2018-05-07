using Avro.Generic;
using CdcTools.KafkaToRedshift.Redshift;
using CdcTools.KafkaToRedshift.Serialization;
using CdcTools.Redshift;
using CdcTools.Redshift.Changes;
using CdcTools.Redshift.S3;
using Confluent.Kafka;
using Confluent.Kafka.Serialization;
using Newtonsoft.Json;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace CdcTools.KafkaToRedshift.Consumers
{
    public class KeyedAvroConsumer : IConsumer
    {
        private IRedshiftWriter _redshiftWriter;
        private List<Task> _consumerTasks;
        private List<Task> _redshiftTasks;
        private string _kafkaBootstrapServers;
        private string _schemaRegistryUrl;

        public KeyedAvroConsumer(IRedshiftWriter redshiftWriter, string kafkaBootstrapServers, string schemaRegistryUrl)
        {
            _redshiftWriter = redshiftWriter;
            _consumerTasks = new List<Task>();
            _redshiftTasks = new List<Task>();

            _kafkaBootstrapServers = kafkaBootstrapServers;
            _schemaRegistryUrl = schemaRegistryUrl;
        }

        public async Task<bool> StartConsumingAsync(CancellationToken token, TimeSpan windowSizePeriod, int windowSizeItems, List<KafkaSource> kafkaSources)
        {
            var columnsLoaded = await CacheRedshiftColumns(kafkaSources.Select(x => x.Table).ToList());
            if (!columnsLoaded)
                return columnsLoaded;

            foreach (var kafkaSource in kafkaSources)
            {
                var accumulatedChanges = new BlockingCollection<MessageProxy<RowChange>>();
                _consumerTasks.Add(Task.Run(() =>
                {
                    try
                    {
                        Consume(token, accumulatedChanges, kafkaSource.Topic, kafkaSource.Table);
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Consumer failure. Table: {kafkaSource.Table}. Error: {ex}");
                    }
                }));

                _redshiftTasks.Add(Task.Run(async () =>
                {
                    try
                    {
                        await _redshiftWriter.StartWritingAsync(token, windowSizePeriod, windowSizeItems, kafkaSource.Table, accumulatedChanges);
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Redshift Writer failure. Table: {kafkaSource.Table}. Error: {ex}");
                    }
                }));
            }

            return columnsLoaded;
        }

        public void WaitForCompletion()
        {
            Task.WaitAll(_consumerTasks.ToArray());
            Task.WaitAll(_redshiftTasks.ToArray());
        }

        private async Task<bool> CacheRedshiftColumns(List<string> tables)
        {
            try
            {
                await _redshiftWriter.CacheTableColumnsAsync(tables);
                return true;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Failed getting Redshift column meta data. {ex}");
                return false;
            }
        }

        private void Consume(CancellationToken token, BlockingCollection<MessageProxy<RowChange>> accumulatedChanges, string topic, string table)
        {
            var conf = new Dictionary<string, object>
            {
                  { "group.id", $"{table}-consumer-group" },
                  { "bootstrap.servers", _kafkaBootstrapServers },
                  { "statistics.interval.ms", 60000 },
                  { "schema.registry.url", _schemaRegistryUrl }
            };

            foreach (var confPair in conf)
                Console.WriteLine(topic + " - " + confPair.Key + ": " + confPair.Value);

            AvroTableTypeConverter avroTableTypeConverter = null;

            using (var consumer = new Consumer<string, GenericRecord>(conf, new StringDeserializer(Encoding.UTF8), new AvroDeserializer<GenericRecord>()))
            {
                consumer.OnError += (_, msg)
                    => Console.WriteLine($"{topic} - Error: {msg.Reason}");

                consumer.OnConsumeError += (_, msg)
                    => Console.WriteLine($"{topic} - Consume error: {msg.Error.Reason}");

                consumer.OnPartitionsAssigned += (_, partitions) =>
                {
                    Console.WriteLine($"{topic} - Assigned partitions: [{string.Join(", ", partitions)}], member id: {consumer.MemberId}");
                    consumer.Assign(partitions);
                };

                consumer.OnPartitionsRevoked += (_, partitions) =>
                {
                    Console.WriteLine($"{topic} - Revoked partitions: [{string.Join(", ", partitions)}]");
                    consumer.Unassign();
                };

                Console.WriteLine($"Subscribing to topic {topic}");
                consumer.Subscribe(topic);
                int secondsWithoutMessage = 0;

                while (!token.IsCancellationRequested)
                {
                    Message<string, GenericRecord> msg = null;
                    if (consumer.Consume(out msg, TimeSpan.FromSeconds(1)))
                    {
                        if (avroTableTypeConverter == null)
                            avroTableTypeConverter = new AvroTableTypeConverter(msg.Value.Schema);
                        else if (!avroTableTypeConverter.SchemaMatches(msg.Value.Schema))
                            avroTableTypeConverter = new AvroTableTypeConverter(msg.Value.Schema);

                        AddToBuffer(consumer, msg, accumulatedChanges, avroTableTypeConverter);
                        secondsWithoutMessage = 0;
                    }
                    else
                    {
                        secondsWithoutMessage++;
                        if (secondsWithoutMessage % 30 == 0)
                            Console.WriteLine($"{topic}: No messages in last {secondsWithoutMessage} seconds");

                        Task.Delay(100).Wait();
                    }
                }
            }

            accumulatedChanges.CompleteAdding(); // notifies consumers that no more messages will come
        }

        private void AddToBuffer(Consumer<string, GenericRecord> consumer, 
            Message<string, GenericRecord> avroMessage, 
            BlockingCollection<MessageProxy<RowChange>> accumulatedChanges,
            AvroTableTypeConverter avroTableTypeConverter)
        {
            var tableChange = avroTableTypeConverter.GetRowChange(avroMessage.Value);
            var msg = new MessageProxy<RowChange>(consumer, avroMessage) { Payload = tableChange };
            accumulatedChanges.Add(msg);
        }
    }
}
