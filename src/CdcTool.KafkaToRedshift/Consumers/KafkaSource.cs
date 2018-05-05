using System;
using System.Collections.Generic;
using System.Text;

namespace CdcTools.KafkaToRedshift.Consumers
{
    public class KafkaSource
    {
        public string Table { get; set; }
        public string Topic { get; set; }
    }
}
