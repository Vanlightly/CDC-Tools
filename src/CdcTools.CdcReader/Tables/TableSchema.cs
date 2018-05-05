using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace CdcTools.CdcReader.Tables
{
    public class TableSchema
    {
        public string Schema { get; set; }
        public string TableName { get; set; }
        public IList<ColumnSchema> Columns { get; set; }
        public IList<PrimaryKeyColumn> PrimaryKeys { get; set; }

        private Dictionary<string, ColumnSchema> _columnsDict;
        public ColumnSchema GetColumn(string columnName)
        {
            if (_columnsDict == null)
            {
                _columnsDict = new Dictionary<string, ColumnSchema>();
                foreach (var col in Columns)
                    _columnsDict.Add(col.Name, col);
            }

            return _columnsDict[columnName];
        }

        public string GetOrderedPrimaryKeyColumns()
        {
            if (PrimaryKeys.Count == 1)
                return PrimaryKeys.First().ColumnName;

            var sb = new StringBuilder();
            int ctr = 0;
            foreach (var pk in PrimaryKeys.OrderBy(x => x.OrdinalPosition))
            {
                if (ctr > 0)
                    sb.Append(",");

                sb.Append(pk.ColumnName);
                ctr++;
            }

            return sb.ToString();
        }
    }
}
