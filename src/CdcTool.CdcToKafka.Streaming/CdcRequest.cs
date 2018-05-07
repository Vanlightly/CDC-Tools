using System;
using System.Collections.Generic;
using System.Text;

namespace CdcTools.CdcToKafka.Streaming
{
    public class CdcRequest
    {
        public string ExecutionId { get; set; }
        public TimeSpan Interval { get; set; }
        public List<string> Tables { get; set; }
        public SerializationMode SerializationMode { get; set; }
        public int BatchSize { get; set; }
        public bool SendWithKey { get; set; }
    }
}
