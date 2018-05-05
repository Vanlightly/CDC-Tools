using CdcTools.CdcReader.Changes;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace CdcTools.CdcToKafka.Streaming.Producers
{
    public interface IKafkaProducer : IDisposable
    {
        Task SendAsync(CancellationToken token, ChangeRecord changeRecord);
    }
}
