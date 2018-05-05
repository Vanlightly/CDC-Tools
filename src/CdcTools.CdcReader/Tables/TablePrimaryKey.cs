using System;
using System.Collections.Generic;
using System.Text;

namespace CdcTools.CdcReader.Tables
{
    internal class TablePrimaryKey
    {
        public string TableName { get; set; }
        public string ColumnName { get; set; }
        public int OrdinalPosition { get; set; }
    }
}
