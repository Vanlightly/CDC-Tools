using CdcTools.CdcReader.Changes;
using System;
using System.Collections.Generic;
using System.Text;

namespace CdcTools.CdcToKafka.Streaming.Producers
{
    public class ProducerBase
    {
        protected Dictionary<string, object> _config;
        protected string _topic;

        public ProducerBase(string topic)
        {
            _topic = topic;
        }

        public RowChange Convert(ChangeRecord changeRecord)
        {
            return new RowChange()
            {
                ChangeType = changeRecord.ChangeType,
                Data = changeRecord.Data,
                ChangeKey = changeRecord.ChangeKey,
                Lsn = changeRecord.LsnStr,
                SeqVal = changeRecord.SeqValStr
            };
        }
    }
}
