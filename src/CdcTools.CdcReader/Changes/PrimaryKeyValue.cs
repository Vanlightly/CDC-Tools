using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace CdcTools.CdcReader.Changes
{
    public class PrimaryKeyValue
    {
        public PrimaryKeyValue()
        {
            Keys = new List<Tuple<int, string, object>>();
        }

        private List<Tuple<int, string, object>> Keys { get; set; }

        public void AddKeyValue(int ordinalPosition, string column, object value)
        {
            Keys.Add(Tuple.Create(ordinalPosition, column, value));
        }

        public object GetValue(int ordinalPosition)
        {
            return Keys.Single(x => x.Item1 == ordinalPosition).Item3;
        }
    }
}
