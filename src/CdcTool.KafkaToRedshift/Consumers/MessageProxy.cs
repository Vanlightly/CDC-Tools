using Avro.Generic;
using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace CdcTools.KafkaToRedshift.Consumers
{
    public class MessageProxy<T>
    {
        private Consumer<Null, string> _consumerNoKey;
        private Consumer<string, string> _consumerWithKey;
        private Consumer<Null, GenericRecord> _genericConsumerNoKey;
        private Consumer<string, GenericRecord> _genericConsumerWithKey;

        private Message<Null, string> _messageNoKey;
        private Message<string, string> _messageWithKey;
        private Message<Null, GenericRecord> _genericMessageNoKey;
        private Message<string, GenericRecord> _genericMessageWithKey;

        public MessageProxy(Consumer<Null, string> consumer, Message<Null, string> message)
        {
            _consumerNoKey = consumer;
            _messageNoKey = message;
        }

        public MessageProxy(Consumer<string, string> consumer, Message<string, string> message)
        {
            _consumerWithKey = consumer;
            _messageWithKey = message;
        }

        public MessageProxy(Consumer<Null, GenericRecord> consumer, Message<Null, GenericRecord> message)
        {
            _genericConsumerNoKey = consumer;
            _genericMessageNoKey = message;
        }

        public MessageProxy(Consumer<string, GenericRecord> consumer, Message<string, GenericRecord> message)
        {
            _genericConsumerWithKey = consumer;
            _genericMessageWithKey = message;
        }

        public T Payload { set; get; }

        public async Task CommitAsync()
        {
            if (_consumerNoKey != null)
                await _consumerNoKey.CommitAsync(_messageNoKey);
            else if (_consumerWithKey != null)
                await _consumerWithKey.CommitAsync(_messageWithKey);
            else if (_genericConsumerNoKey != null)
                await _genericConsumerNoKey.CommitAsync(_genericMessageNoKey);
            else if (_genericConsumerWithKey != null)
                await _genericConsumerWithKey.CommitAsync(_genericMessageWithKey);
        }
    }
}
