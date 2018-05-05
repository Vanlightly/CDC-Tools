using CdcTools.CdcToKafka.Streaming.Serialization;
using CdcTools.CdcReader.Tables;
using CdcTools.SqlToAvro;
using System;
using System.Collections.Generic;
using System.Text;

namespace CdcTools.CdcToKafka.Streaming.Producers
{
    internal class ProducerFactory
    {
        public static IKafkaProducer GetProducer(string topic, TableSchema tableSchema, SerializationMode serializationMode, bool sendWithKey)
        {
            if (serializationMode == SerializationMode.Avro)
            {
                var avroSchema = AvroSchemaGenerator.GenerateSchema("CdcToRedshift", tableSchema);

                if (sendWithKey)
                    return new KeyedAvroProducer(topic, new AvroTableTypeConverter(avroSchema), tableSchema);
                else
                    return new NonKeyedAvroProducer(topic, new AvroTableTypeConverter(avroSchema), tableSchema);
            }
            else
            {
                if (sendWithKey)
                    return new KeyedJsonProducer(topic);
                else
                    return new NonKeyedJsonProducer(topic);
            }
        }
    }
}
