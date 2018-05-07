using Avro;
using Avro.Generic;
using CdcTools.CdcReader.Tables;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace CdcTools.CdcToKafka.Streaming.Serialization
{
    public class AvroTableTypeConverter
    {
        private DateTime Epoch = new DateTime(1970, 1, 1);
        private RecordSchema _schema;
        private JObject _schemaJson;
        private Dictionary<string, string> _fieldConversions;

        public AvroTableTypeConverter(string schema)
        {
            _schema = (RecordSchema)Schema.Parse(schema);
            _schemaJson = JObject.Parse(schema);

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

        public GenericRecord GetRecord(TableSchema tableSchema, RowChange tableChange)
        {
            var record = new GenericRecord(_schema);
            record.Add("Lsn", tableChange.Lsn);
            record.Add("SeqVal", tableChange.SeqVal);
            record.Add("ChangeKey", tableChange.ChangeKey);
            record.Add("ChangeType", (int)tableChange.ChangeType);

            foreach (var column in tableSchema.Columns.OrderBy(x => x.OrdinalPosition))
            {
                var value = tableChange.Data[column.Name.ToLower()];
                switch (column.DataType)
                {
                    case "varchar":
                    case "nvarchar":
                    case "char":
                    case "nchar":
                    case "bigint":
                    case "binary":
                    case "varbinary":
                    case "text":
                    case "image":
                        record.Add(column.Name, value);
                        break;
                    case "uniqueidentifier":
                        record.Add(column.Name, ((Guid)value).ToString());
                        break;
                    case "decimal":
                        record.Add(column.Name, BitConverter.GetBytes(Convert.ToDouble((decimal)value)));
                        break;
                    case "tinyint":
                        record.Add(column.Name, (int)(byte)value);
                        break;
                    case "smallint":
                        record.Add(column.Name, (int)(short)value);
                        break;
                    case "int":
                        record.Add(column.Name, (int)value);
                        break;
                    case "date":
                        record.Add(column.Name, (int)((DateTime)value - Epoch).TotalDays);
                        break;
                    case "datetime":
                        record.Add(column.Name, (long)((DateTime)value - Epoch).TotalMilliseconds);
                        break;
                    case "datetime2":
                        record.Add(column.Name, (((DateTime)value).Ticks - Epoch.Ticks) / 10);
                        break;
                    case "time":
                        record.Add(column.Name, (int)((TimeSpan)value).TotalMilliseconds);
                        break;
                    default:
                        throw new Exception("Unsupported type: " + column.DataType);
                }
            }

            return record;
        }
    }
}
