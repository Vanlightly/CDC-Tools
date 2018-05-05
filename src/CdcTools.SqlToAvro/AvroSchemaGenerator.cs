using CdcTools.CdcReader.Tables;
using Newtonsoft.Json.Linq;
using System;
using System.Linq;

namespace CdcTools.SqlToAvro
{
    public class AvroSchemaGenerator
    {
        public static string GenerateSchema(string schemaNamespace, TableSchema tableSchema)
        {
            var schema = new JObject();
            schema["namespace"] = schemaNamespace;
            schema["name"] = tableSchema.TableName;
            schema["type"] = "record";

            var fields = new JArray();
            var lsnField = new JObject();
            lsnField["name"] = "Lsn";
            lsnField["type"] = "string";
            fields.Add(lsnField);

            var seqValField = new JObject();
            seqValField["name"] = "SeqVal";
            seqValField["type"] = "string";
            fields.Add(seqValField);

            var keyField = new JObject();
            keyField["name"] = "ChangeKey";
            keyField["type"] = "string";
            fields.Add(keyField);

            var changeTypeField = new JObject();
            changeTypeField["name"] = "ChangeType";
            changeTypeField["type"] = "int";
            fields.Add(changeTypeField);

            foreach (var column in tableSchema.Columns.OrderBy(x => x.OrdinalPosition))
            {
                var field = new JObject();
                field["name"] = column.Name;

                switch (column.DataType)
                {
                    case "varchar":
                    case "nvarchar":
                    case "char":
                    case "nchar":
                    case "text":
                    case "uniqueidentifier":
                        field["type"] = "string";
                        break;
                    case "bigint":
                        field["type"] = "long";
                        break;
                    case "binary":
                    case "varbinary":
                    case "image":
                        field["type"] = "bytes";
                        break;
                    case "decimal":
                        field["type"] = "bytes";
                        field["logicalType"] = "decimal";
                        field["precision"] = column.Precision;
                        field["scale"] = column.Scale;
                        break;
                    case "tinyint":
                    case "smallint":
                    case "int":
                        field["type"] = "int";
                        break;
                    case "date":
                        field["type"] = "int";
                        field["logicalType"] = "date";
                        break;
                    case "datetime":
                        field["type"] = "long";
                        field["logicalType"] = "timestamp-millis";
                        break;
                    case "datetime2":
                        field["type"] = "long";
                        field["logicalType"] = "timestamp-micros";
                        break;
                    case "time":
                        field["type"] = "long";
                        field["logicalType"] = "timestamp-millis";
                        break;
                }

                fields.Add(field);
            }

            schema["fields"] = fields;

            return schema.ToString();
        }
    }
}
