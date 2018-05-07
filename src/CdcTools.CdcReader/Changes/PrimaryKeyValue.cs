using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace CdcTools.CdcReader.Changes
{
    public class KeyColumnValue
    {
        public KeyColumnValue(int ordinalPosition, string columnName, object value)
        {
            OrdinalPosition = ordinalPosition;
            ColumnName = columnName;
            Value = value;
        }

        public int OrdinalPosition { get; set; }
        public string ColumnName { get; set; }
        public object Value { get; set; }
    }

    public class PrimaryKeyValue
    {
        public PrimaryKeyValue()
        {
            Keys = new List<KeyColumnValue>();
        }

        public List<KeyColumnValue> Keys { get; set; }

        public void AddKeyValue(int ordinalPosition, string column, object value)
        {
            Keys.Add(new KeyColumnValue(ordinalPosition, column, value));
        }

        public object GetValue(int ordinalPosition)
        {
            return Keys.Single(x => x.OrdinalPosition == ordinalPosition).Value;
        }
    }
}
