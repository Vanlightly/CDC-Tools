using Avro;
using Avro.Generic;
using CdcTools.Redshift.Changes;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace CdcTools.KafkaToRedshift.Serialization
{
    public class AvroTableTypeConverter
    {
        private DateTime Epoch = new DateTime(1970, 1, 1);
        private string _schemaText;
        private RecordSchema _schema;
        private JObject _schemaJson;
        private Dictionary<string, string> _fieldConversions;

        public AvroTableTypeConverter(RecordSchema schema)
        {
            _schemaText = schema.ToString();
            _schema = schema;
            _schemaJson = JObject.Parse(_schemaText);

            var fields = (JArray)_schemaJson["fields"];
            _fieldConversions = new Dictionary<string, string>();
            foreach (var field in fields)
            {
                if (field.Children().Any(x => x.Path.EndsWith("logicalType")))
                    _fieldConversions.Add(field["name"].Value<string>(), field["logicalType"].Value<string>());
                else
                    _fieldConversions.Add(field["name"].Value<string>(), "");
            }
        }

        public RowChange GetRowChange(GenericRecord record)
        {
            var change = new RowChange();
            change.Lsn = (string)record["Lsn"];
            change.SeqVal = (string)record["SeqVal"];
            change.ChangeKey = (string)record["ChangeKey"];
            change.ChangeType = (ChangeType)(int)record["ChangeType"];

            foreach (var fieldPair in _fieldConversions)
            {
                switch (fieldPair.Value)
                {
                    case "":
                        change.Data.Add(fieldPair.Key, record[fieldPair.Key]);
                        break;
                    case "decimal":
                        var decBytes = (byte[])record[fieldPair.Key];
                        var dec = Convert.ToDecimal(BitConverter.ToDouble(decBytes, 0));
                        change.Data.Add(fieldPair.Key, dec);
                        break;
                    case "date":
                        var date = new DateTime(1970, 1, 1).AddDays((int)record[fieldPair.Key]);
                        change.Data.Add(fieldPair.Key, date);
                        break;
                    case "timestamp-millis":
                        var datetime = new DateTime(1970, 1, 1).AddMilliseconds((long)record[fieldPair.Key]);
                        change.Data.Add(fieldPair.Key, datetime);
                        break;
                    case "timestamp-micros":
                        var datetimeMicro = new DateTime(Epoch.Ticks + ((long)record[fieldPair.Key] * 10));
                        change.Data.Add(fieldPair.Key, datetimeMicro);
                        break;
                    case "time-millis":
                        var timeMilli = TimeSpan.FromMilliseconds((int)record[fieldPair.Key]);
                        change.Data.Add(fieldPair.Key, timeMilli);
                        break;
                    case "time-micros":
                        var timeMicro = TimeSpan.FromTicks((long)record[fieldPair.Key] * 10);
                        change.Data.Add(fieldPair.Key, timeMicro);
                        break;

                }
            }

            return change;
        }

        public bool SchemaMatches(RecordSchema schema)
        {
            return _schema.Equals(schema);
        }
    }
}
