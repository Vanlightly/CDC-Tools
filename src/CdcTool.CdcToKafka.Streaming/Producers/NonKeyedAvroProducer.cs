using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using CdcTools.CdcReader.Changes;
using CdcTools.CdcToKafka.Streaming.Serialization;
using CdcTools.CdcReader.Tables;
using Confluent.Kafka;
using Avro.Generic;
using Confluent.Kafka.Serialization;

namespace CdcTools.CdcToKafka.Streaming.Producers
{
    public class NonKeyedAvroProducer : ProducerBase, IKafkaProducer
    {
        private AvroTableTypeConverter _avroTypeConverter;
        private TableSchema _tableSchema;
        private Producer<Null, GenericRecord> _producer;

        public NonKeyedAvroProducer(string bootstrapServers, string schemaRegistryUrl, string topic, AvroTableTypeConverter avroTableTypeConverter, TableSchema tableSchema)
            : base(topic)
        {
            _config = new Dictionary<string, object>
            {
                { "bootstrap.servers", bootstrapServers },
                { "schema.registry.url", schemaRegistryUrl },
                { "socket.blocking.max.ms", "1" } // workaround for https://github.com/confluentinc/confluent-kafka-dotnet/issues/501
            };

            _producer = new Producer<Null, GenericRecord>(_config, null, new AvroSerializer<GenericRecord>());
            _avroTypeConverter = avroTableTypeConverter;
            _tableSchema = tableSchema;
        }

        public async Task SendAsync(CancellationToken token, ChangeRecord changeRecord)
        {
            var change = Convert(changeRecord);
            var record = _avroTypeConverter.GetRecord(_tableSchema, change);
            var sent = false;
            while (!sent && !token.IsCancellationRequested)
            {
                var sendResult = await _producer.ProduceAsync(topic: _topic, key: null, val: record, blockIfQueueFull: true);
                if (sendResult.Error.HasError)
                {
                    Console.WriteLine("Could not send: " + sendResult.Error.Reason);
                    await Task.Delay(100);
                }
                else
                    sent = true;
            }
        }

        private bool _disposed;
        public void Dispose()
        {
            if (!_disposed)
            {
                if (_producer != null)
                    _producer.Dispose();

                _disposed = true;
            }
        }
    }
}
