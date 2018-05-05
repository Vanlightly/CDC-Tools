using System;
using System.Collections.Generic;
using System.Text;

namespace CdcTools.CdcReader.Tables
{
    public class ColumnSchema
    {
        public int OrdinalPosition { get; set; }
        public string Name { get; set; }
        public string DataType { get; set; }
        public int MaxCharsLength { get; set; }
        public bool IsNullable { get; set; }
        public int Precision { get; set; }
        public int Scale { get; set; }

        public Type GetNetDataType()
        {
            switch (DataType)
            {
                case "char":
                    return typeof(string);
                case "varchar":
                    return typeof(string);
                case "nvarchar":
                    return typeof(string);
                case "tinyint":
                    return typeof(byte);
                case "smallint":
                    return typeof(short);
                case "int":
                    return typeof(int);
                case "bigint":
                    return typeof(long);
                case "date":
                    return typeof(DateTime);
                case "datetime":
                    return typeof(DateTime);
                case "datetime2":
                    return typeof(DateTime);
                case "time":
                    return typeof(DateTime);
                case "bit":
                    return typeof(bool);
                case "money":
                    return typeof(decimal);
                case "uniqueidentifier":
                    return typeof(Guid);
                case "varbinary":
                    return typeof(byte[]);
                default:
                    throw new Exception("SQL data type not supported: " + DataType);
            }
        }
    }
}
