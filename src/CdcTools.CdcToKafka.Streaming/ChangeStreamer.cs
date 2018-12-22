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
using CdcTools.CdcReader.State;
using CdcTools.CdcReader.Tables;

namespace CdcTools.CdcToKafka.Streaming
{
    public class ChangeStreamer
    {
        private string _tableTopicPrefix;
        private string _kafkaBootstrapServers;
        private string _schemaRegistryUrl;
        private CdcReaderClient _cdcReaderClient;
        private List<Task> _readerTasks;

        public ChangeStreamer(IConfiguration configuration, CdcReaderClient cdcReaderClient)
        {
            _cdcReaderClient = cdcReaderClient;
            _tableTopicPrefix = configuration["TableTopicPrefix"];
            _kafkaBootstrapServers = configuration["KafkaBootstrapServers"];
            _schemaRegistryUrl = configuration["KafkaSchemaRegistryUrl"];

            _readerTasks = new List<Task>();
        }

        public void StartReading(CancellationToken token, CdcRequest cdcRequest)
        {
            foreach (var tableName in cdcRequest.Tables)
            {
                var readerTask = Task.Run(async () =>
                {
                    try
                    {
                        await StartPublishingChanges(token,
                            cdcRequest.ExecutionId,
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
            string executionId,
            string tableName, 
            TimeSpan maxInterval, 
            int batchSize, 
            bool sendWithKey, 
            SerializationMode serializationMode)
        {
            var tableTopic = _tableTopicPrefix + tableName.ToLower();
            var tableSchema = await _cdcReaderClient.GetTableSchemaAsync(tableName);
                        
            using (var producer = ProducerFactory.GetProducer(tableTopic, tableSchema, serializationMode, sendWithKey, _kafkaBootstrapServers, _schemaRegistryUrl))
            {
                var cdcState = await SetInitialStateAsync(token, producer, executionId, tableSchema, maxInterval);
                var sw = new Stopwatch();

                while (!token.IsCancellationRequested)
                {
                    cdcState.ToLsn = await _cdcReaderClient.GetMaxLsnAsync();
                    sw.Start();
                    Console.WriteLine($"Table {tableName} - Starting to export LSN range {GetBigInteger(cdcState.FromLsn)} to {GetBigInteger(cdcState.ToLsn)}");

                    bool more = true;
                    int blockCounter = 0;
                    while (!token.IsCancellationRequested && more)
                    {
                        if (GetBigInteger(cdcState.FromLsn) <= GetBigInteger(cdcState.ToLsn))
                        {
                            blockCounter++;
                            ChangeBatch batch = null;
                            if (cdcState.UnfinishedLsn)
                                batch = await _cdcReaderClient.GetChangeBatchAsync(tableSchema, cdcState.FromLsn, cdcState.FromSeqVal, cdcState.ToLsn, batchSize);
                            else
                                batch = await _cdcReaderClient.GetChangeBatchAsync(tableSchema, cdcState.FromLsn, cdcState.ToLsn, batchSize);

                            if (batch.Changes.Any())
                            {
                                Console.WriteLine($"Table {tableName} - Retrieved block #{blockCounter} with {batch.Changes.Count} changes");
                                foreach (var change in batch.Changes)
                                {
                                    await producer.SendAsync(token, change);

                                    cdcState.FromLsn = change.Lsn;
                                    cdcState.FromSeqVal = change.SeqVal;
                                }

                                more = batch.MoreChanges;
                                cdcState.UnfinishedLsn = batch.MoreOfLastTransaction;

                                if (cdcState.UnfinishedLsn)
                                    cdcState.FromSeqVal = Increment(cdcState.FromSeqVal);
                                else
                                    cdcState.FromLsn = Increment(cdcState.FromLsn);

                                var offset = GetOffset(cdcState);
                                await BlockingStoreCdcOffsetAsync(token, executionId, tableName, offset);
                            }
                            else
                            {
                                more = false;
                                cdcState.UnfinishedLsn = false;
                                Console.WriteLine($"Table {tableName} - No changes");
                            }
                        }
                        else
                        {
                            more = false;
                            cdcState.UnfinishedLsn = false;
                            Console.WriteLine($"Table {tableName} - No changes");
                        }
                    }

                    var remainingMs = maxInterval.TotalMilliseconds - sw.Elapsed.TotalMilliseconds;
                    if (remainingMs > 0)
                        await Task.Delay((int)remainingMs);

                    sw.Reset();
                }
            }
        }

        private async Task<CdcState> SetInitialStateAsync(CancellationToken token, IKafkaProducer producer, string executionId, TableSchema tableSchema, TimeSpan maxInterval)
        {
            byte[] initialToLsn = await _cdcReaderClient.GetMaxLsnAsync();

            var existingOffset = await _cdcReaderClient.GetLastCdcOffsetAsync(executionId, tableSchema.TableName);
            if (existingOffset.Result == Result.NoStoredState)
            {
                Console.WriteLine($"Table {tableSchema.TableName} - No previous stored LSN. Starting from first change");
                
                var hasFirstChange = false;
                ChangeBatch syncBatch = null;
                ChangeRecord firstChange = null;
                while (!hasFirstChange && !token.IsCancellationRequested)
                {
                    var initialFromLsn = await _cdcReaderClient.GetMinValidLsnAsync(tableSchema.TableName);
                    initialToLsn = await _cdcReaderClient.GetMaxLsnAsync();

                    syncBatch = await _cdcReaderClient.GetChangeBatchAsync(tableSchema, initialFromLsn, initialToLsn, 1);
                    if (syncBatch.Changes.Any())
                    {
                        firstChange = syncBatch.Changes.First();
                        // await producer.SendAsync(token, firstChange);
                        hasFirstChange = true;
                    }
                    else {
                        await Task.Delay(maxInterval);
                    }
                }

                var cdcState = new CdcState()
                {
                    FromLsn = firstChange.Lsn,
                    FromSeqVal = firstChange.SeqVal,
                    ToLsn = initialToLsn,
                    UnfinishedLsn = syncBatch.MoreOfLastTransaction
                };

                // var offset = GetOffset(cdcState);
                // await _cdcReaderClient.StoreCdcOffsetAsync(executionId, tableSchema.TableName, offset);

                return cdcState;
            }
            else
            {
                Console.WriteLine($"Table {tableSchema.TableName} - Starting from stored LSN");

                return new CdcState()
                {
                    FromLsn = existingOffset.State.Lsn,
                    FromSeqVal = existingOffset.State.SeqVal,
                    ToLsn = initialToLsn,
                    UnfinishedLsn = existingOffset.State.UnfinishedLsn
                };
            }
        }

        private Offset GetOffset(CdcState cdcState)
        {
            var offset = new Offset();
            offset.Lsn = cdcState.FromLsn;
            offset.SeqVal = cdcState.FromSeqVal;
            offset.UnfinishedLsn = cdcState.UnfinishedLsn;

            return offset;
        }

        private async Task BlockingStoreCdcOffsetAsync(CancellationToken token, string executionId, string tableName, Offset offset)
        {
            var stored = false;

            while (!stored && !token.IsCancellationRequested)
            {
                try
                {
                    await _cdcReaderClient.StoreCdcOffsetAsync(executionId, tableName, offset);
                    stored = true;
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Table {tableName} - Could not store LSN. {ex}");
                    await WaitForSeconds(token, 10);
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

        private async Task WaitForSeconds(CancellationToken token, int seconds)
        {
            int waited = 0;

            while (waited < seconds && !token.IsCancellationRequested)
            {
                await Task.Delay(1000);
                waited++;
            }
        }
    }
}
