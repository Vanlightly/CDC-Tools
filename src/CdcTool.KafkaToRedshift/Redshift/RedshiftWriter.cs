using CdcTools.KafkaToRedshift.Consumers;
using CdcTools.Redshift;
using CdcTools.Redshift.Changes;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace CdcTools.KafkaToRedshift.Redshift
{
    public class RedshiftWriter : IRedshiftWriter
    {
        private RedshiftClient _redshiftClient;
        
        public RedshiftWriter(RedshiftClient redshiftClient)
        {
            _redshiftClient = redshiftClient;
        }

        public async Task CacheTableColumnsAsync(List<string> tables)
        {
            await _redshiftClient.CacheTableColumnsAsync(tables);
        }

        public async Task StartWritingAsync(CancellationToken token, TimeSpan windowSizePeriod, int windowSizeItems, string table, BlockingCollection<MessageProxy<RowChange>> accumulatedChanges)
        {
            await WriteToRedshiftAsync(token, windowSizePeriod, windowSizeItems, table, accumulatedChanges);
        }

        private async Task WriteToRedshiftAsync(CancellationToken token, TimeSpan windowSize, int windowSizeItems, string tableName, BlockingCollection<MessageProxy<RowChange>> accumulatedChanges)
        {
            tableName = tableName.ToLower();

            while (!token.IsCancellationRequested && !accumulatedChanges.IsAddingCompleted)
            {
                // create change window
                var messages = EmptyBuffer(accumulatedChanges);
                if (messages.Any())
                {
                    var changesToPut = messages.Select(x => x.Payload).ToList();

                    // upload change window to S3 then Redshift
                    await _redshiftClient.UploadAsCsvAsync(tableName, changesToPut);

                    // commit the last message in the batch
                    await messages.Last().CommitAsync();
                }

                // wait for interval but check buffered item count regularly and if max size is reached then upload
                int secondsWaited = 0;
                while(secondsWaited < (int)windowSize.TotalSeconds)
                {
                    if (accumulatedChanges.Count >= windowSizeItems)
                        break;

                    secondsWaited++;
                    await Task.Delay(1000);
                }
            }
        }

        private List<MessageProxy<RowChange>> EmptyBuffer(BlockingCollection<MessageProxy<RowChange>> accumulatedChanges)
        {
            var changesToPut = new List<MessageProxy<RowChange>>();

            MessageProxy<RowChange> change = null;
            while (accumulatedChanges.TryTake(out change))
                changesToPut.Add(change);

            return changesToPut;
        }
    }
}
