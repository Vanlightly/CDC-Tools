using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace CdcTools.KafkaToRedshift.Consumers
{
    public interface IConsumer
    {
        Task StartConsumingAsync(CancellationToken token, TimeSpan windowSize, List<KafkaSource> kafkaSources);
        void WaitForCompletion();
    }
}
