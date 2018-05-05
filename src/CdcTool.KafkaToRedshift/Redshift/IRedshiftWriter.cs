using CdcTools.KafkaToRedshift.Consumers;
using CdcTools.Redshift.Changes;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace CdcTools.KafkaToRedshift.Redshift
{
    public interface IRedshiftWriter
    {
        Task CacheTableColumnsAsync(List<string> tables);
        Task StartWritingAsync(CancellationToken token, TimeSpan windowSizePeriod, int windowSizeItems, string table, BlockingCollection<MessageProxy<RowChange>> accumulatedChanges);
    }
}
