using System;
using System.Collections.Generic;
using System.Text;

namespace CdcTools.CdcReader.Tables
{
    internal class TableColumn
    {
        public string Schema { get; set; }
        public string TableName { get; set; }
        public int OrdinalPosition { get; set; }
        public string ColumnName { get; set; }
        public string DataType { get; set; }
        public int MaxCharsLength { get; set; }
        public bool IsNullable { get; set; }
        public int NumericScale { get; set; }
        public int NumericPrecision { get; set; }
    }
}
