using CdcTools.KafkaToRedshift.Redshift;
using CdcTools.Redshift.Changes;
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
    public class KeyedJsonConsumer : IConsumer
    {
        private IRedshiftWriter _redshiftWriter;
        private List<Task> _consumerTasks;
        private List<Task> _redshiftTasks;

        public KeyedJsonConsumer(IRedshiftWriter redshiftClient)
        {
            _redshiftWriter = redshiftClient;
            _consumerTasks = new List<Task>();
            _redshiftTasks = new List<Task>();
        }

        public async Task StartConsumingAsync(CancellationToken token, TimeSpan windowSize, List<KafkaSource> kafkaSources)
        {
            await _redshiftWriter.CacheTableColumnsAsync(kafkaSources.Select(x => x.Table).ToList());

            foreach (var kafkaSource in kafkaSources)
            {
                var accumulatedChanges = new BlockingCollection<MessageProxy<RowChange>>();
                _consumerTasks.Add(Task.Run(() =>
                {
                    try
                    {
                        Consume(token, accumulatedChanges, kafkaSource.Topic, kafkaSource.Table);
                    }
                    catch(Exception ex)
                    {
                        Console.WriteLine($"Consumer failure. Table: {kafkaSource.Table}. Error: {ex}");
                    }
                }));

                _redshiftTasks.Add(Task.Run(async () =>
                {
                    try
                    {
                        await _redshiftWriter.StartWritingAsync(token, windowSize, kafkaSource.Table, accumulatedChanges);
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Redshift Writer failure. Table: {kafkaSource.Table}. Error: {ex}");
                    }
                }));
            }
        }

        public void WaitForCompletion()
        {
            Task.WaitAll(_consumerTasks.ToArray());
            Task.WaitAll(_redshiftTasks.ToArray());
        }

        private void Consume(CancellationToken token, BlockingCollection<MessageProxy<RowChange>> acuumulatedChanges, string topic, string table)
        {
            var conf = new Dictionary<string, object>
            {
                  { "group.id", $"{table}-consumer-group" },
                  { "bootstrap.servers", "localhost:9092" }
            };

            using (var consumer = new Consumer<string, string>(conf, new StringDeserializer(Encoding.UTF8), new StringDeserializer(Encoding.UTF8)))
            {
                consumer.OnMessage += (_, msg) => AddToBuffer(consumer, msg, acuumulatedChanges);

                consumer.OnError += (_, error)
                  => Console.WriteLine($"Error: {error}");

                consumer.OnConsumeError += (_, msg)
                  => Console.WriteLine($"Consume error ({msg.TopicPartitionOffset}): {msg.Error}");

                consumer.Subscribe(topic);

                while (!token.IsCancellationRequested)
                {
                    consumer.Poll(TimeSpan.FromMilliseconds(100));
                }
            }

            acuumulatedChanges.CompleteAdding(); // notifies consumers that no more messages will come
        }

        private void AddToBuffer(Consumer<string, string> consumer, Message<string, string> jsonMessage, BlockingCollection<MessageProxy<RowChange>> accumulatedChanges)
        {
            var msg = new MessageProxy<RowChange>(consumer, jsonMessage)
            {
                Payload = JsonConvert.DeserializeObject<RowChange>(jsonMessage.Value)
            };
            accumulatedChanges.Add(msg);
        }
    }
}
