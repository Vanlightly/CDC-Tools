using CdcTools.CdcToKafka.Streaming.Producers;
using CdcTools.CdcReader;
using CdcTools.CdcReader.Changes;
using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Numerics;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace CdcTools.CdcToKafka.Streaming
{
    public class ChangeStreamer
    {
        private string _tableTopicPrefix;
        private CdcReaderClient _cdcReaderClient;
        private List<Task> _readerTasks;

        public ChangeStreamer(IConfiguration configuration)
        {
            _cdcReaderClient = new CdcReaderClient(configuration["database-connection"]);
            _tableTopicPrefix = configuration["tableTopicPrefix"];

            _readerTasks = new List<Task>();
        }

        public void StartReading(CancellationToken token, CdcRequest cdcRequest)
        {
            foreach (var table in cdcRequest.Tables)
            {
                var schemaName = table.Contains(".") ? table.Substring(0, table.IndexOf(".")) : "dbo";
                var tableName = table.Contains(".") ? table.Substring(table.IndexOf(".") + 1) : table;

                var readerTask = Task.Run(async () =>
                {
                    try
                    {
                        await StartPublishingChanges(token,
                            schemaName,
                            tableName,
                            cdcRequest.Interval,
                            cdcRequest.BatchSize,
                            cdcRequest.SendWithKey,
                            cdcRequest.SerializationMode);
                    }
                    catch(Exception ex)
                    {
                        Console.WriteLine($"CDC reader failure. Table {tableName}. Error: {ex}");
                    }
                });
                _readerTasks.Add(readerTask);
            }
        }

        public void WaitForCompletion()
        {
            Task.WaitAll(_readerTasks.ToArray());
        }

        private async Task StartPublishingChanges(CancellationToken token, 
            string schemaName, 
            string tableName, 
            TimeSpan maxInterval, 
            int batchSize, 
            bool sendWithKey, 
            SerializationMode serializationMode)
        {
            var tableTopic = _tableTopicPrefix + tableName.ToLower();
            var tableSchema = await _cdcReaderClient.GetTableSchemaAsync(schemaName, tableName);
            var initialFromLsn = await _cdcReaderClient.GetMinValidLsnAsync(tableName);
            var initialToLsn = await _cdcReaderClient.GetMaxLsnAsync();
            
            using (var producer = ProducerFactory.GetProducer(tableTopic, tableSchema, serializationMode, sendWithKey))
            {
                var syncBatch = await _cdcReaderClient.GetChangeBatchAsync(tableSchema, initialFromLsn, initialToLsn, 1);
                var firstChange = syncBatch.Changes.First();

                await producer.SendAsync(token, firstChange);

                var crossTranBatch = syncBatch.MoreOfLastTransaction;
                var fromLsn = firstChange.Lsn;
                var fromSeqVal = firstChange.SeqVal;
                var toLsn = initialToLsn;
                var sw = new Stopwatch();

                while (!token.IsCancellationRequested)
                {
                    toLsn = await _cdcReaderClient.GetMaxLsnAsync();
                    sw.Start();
                    Console.WriteLine($"{tableName} batch from LSN {GetBigInteger(fromLsn)} to {GetBigInteger(toLsn)}");

                    bool more = true;
                    while (!token.IsCancellationRequested && more)
                    {
                        if (GetBigInteger(fromLsn) <= GetBigInteger(toLsn))
                        {
                            ChangeBatch batch = null;
                            if (crossTranBatch)
                                batch = await _cdcReaderClient.GetChangeBatchAsync(tableSchema, fromLsn, fromSeqVal, toLsn, batchSize);
                            else
                                batch = await _cdcReaderClient.GetChangeBatchAsync(tableSchema, fromLsn, toLsn, batchSize);

                            if (batch.Changes.Any())
                            {
                                Console.WriteLine($"{DateTime.Now.ToString("HH:mm:ss")} Retrieved {batch.Changes.Count} changes for publishing");
                                foreach (var change in batch.Changes)
                                {
                                    await producer.SendAsync(token, change);

                                    fromLsn = change.Lsn;
                                    fromSeqVal = change.SeqVal;
                                }

                                more = batch.MoreChanges;
                                crossTranBatch = batch.MoreOfLastTransaction;

                                if (crossTranBatch)
                                    fromSeqVal = Increment(fromSeqVal);
                                else
                                    fromLsn = Increment(fromLsn);
                            }
                            else
                            {
                                more = false;
                                crossTranBatch = false;
                                Console.WriteLine($"{DateTime.Now.ToString("HH:mm:ss")} No changes");
                            }
                        }
                        else
                        {
                            more = false;
                            crossTranBatch = false;
                            Console.WriteLine($"{DateTime.Now.ToString("HH:mm:ss")} No changes");
                        }
                    }

                    var remainingMs = maxInterval.TotalMilliseconds - sw.Elapsed.TotalMilliseconds;
                    if (remainingMs > 0)
                        await Task.Delay((int)remainingMs);

                    sw.Reset();
                }
            }
        }

        private byte[] Increment(byte[] lsn)
        {
            var fromLsnInt = new BigInteger(lsn.Reverse().ToArray());
            fromLsnInt++;
            var newFromLsn = fromLsnInt.ToByteArray();
            for (int i = 0; i < 10; i++)
            {
                if (i >= newFromLsn.Length)
                    lsn[9 - i] = 0;
                else
                    lsn[9 - i] = newFromLsn[i];
            }

            return lsn;
        }

        private BigInteger GetBigInteger(byte[] lsn)
        {
            return new BigInteger(lsn.Reverse().ToArray());
        }
    }
}
