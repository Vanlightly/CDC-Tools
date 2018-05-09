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
using CdcTools.Redshift;
using CdcTools.Redshift.Changes;
using CdcTools.CdcReader.State;
using CdcTools.CdcReader.Tables;

namespace CdcTools.CdcToRedshift.NonTransactional
{
    public class ChangeExporter
    {
        private CdcReaderClient _cdcReaderClient;
        private RedshiftClient _redshiftClient;
        private List<Task> _readerTasks;
        private byte[] _zeroValue = new byte[10];

        public ChangeExporter(CdcReaderClient cdcReaderClient,
            RedshiftClient redshiftClient)
        {
            _cdcReaderClient = cdcReaderClient;
            _redshiftClient = redshiftClient;
            _readerTasks = new List<Task>();
        }

        public async Task StartExportingChangesAsync(CancellationToken token, string executionId, List<string> tables, TimeSpan interval, int batchSize)
        {
            await _redshiftClient.CacheTableColumnsAsync(tables);

            foreach (var tableName in tables)
            {
                var readerTask = Task.Run(async () =>
                {
                    while (!token.IsCancellationRequested)
                    {
                        try
                        {
                            await StartPublishingChanges(token,
                                executionId,
                                tableName,
                                interval,
                                batchSize);
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"CDC reader failure. Table {tableName}. Will restart in 30 seconds. Error: {ex}");
                            await WaitForSeconds(token, 30);
                        }
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
            int batchSize)
        {
            var tableSchema = await _cdcReaderClient.GetTableSchemaAsync(tableName);
            var cdcState = await SetInitialStateAsync(token, executionId, tableSchema, maxInterval);

            var sw = new Stopwatch();

            while (!token.IsCancellationRequested)
            {
                cdcState.ToLsn = await _cdcReaderClient.GetMaxLsnAsync();
                sw.Start();
                Console.WriteLine($"Table {tableName} - Starting to export LSN range {GetBigInteger(cdcState.FromLsn)} to {GetBigInteger(cdcState.ToLsn)}");

                int blockCounter = 0;
                bool more = true;
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
                            await BlockingWriteToRedshiftAsync(token, tableSchema.TableName, batch);

                            cdcState.FromLsn = batch.Changes.Last().Lsn;
                            cdcState.FromSeqVal = batch.Changes.Last().SeqVal;
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

        private async Task<CdcState> SetInitialStateAsync(CancellationToken token, string executionId, TableSchema tableSchema, TimeSpan maxInterval)
        {
            byte[] initialToLsn = await _cdcReaderClient.GetMaxLsnAsync();

            var existingOffset = await _cdcReaderClient.GetLastCdcOffsetAsync(executionId, tableSchema.TableName);
            if (existingOffset.Result == Result.NoStoredState)
            {
                Console.WriteLine($"Table {tableSchema.TableName} - No previous stored offset. Starting from first change");
                var initialFromLsn = await _cdcReaderClient.GetMinValidLsnAsync(tableSchema.TableName);

                var hasFirstChange = false;
                ChangeBatch syncBatch = null;
                ChangeRecord firstChange = null;
                while (!hasFirstChange && !token.IsCancellationRequested)
                {
                    syncBatch = await _cdcReaderClient.GetChangeBatchAsync(tableSchema, initialFromLsn, initialToLsn, 1);
                    if (syncBatch.Changes.Any())
                    {
                        firstChange = syncBatch.Changes.First();
                        hasFirstChange = true;
                    }
                    else
                        await Task.Delay(maxInterval);
                }

                await BlockingWriteToRedshiftAsync(token, tableSchema.TableName, syncBatch);

                var cdcState = new CdcState()
                {
                    FromLsn = firstChange.Lsn,
                    FromSeqVal = firstChange.SeqVal,
                    ToLsn = initialToLsn,
                    UnfinishedLsn = syncBatch.MoreOfLastTransaction
                };

                var offset = GetOffset(cdcState);
                await _cdcReaderClient.StoreCdcOffsetAsync(executionId, tableSchema.TableName, offset);

                return cdcState;
            }
            else
            {
                Console.WriteLine($"Table {tableSchema.TableName} - Starting from stored offset");

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
                    Console.WriteLine($"Table {tableName} - Could not store offset. {ex}");
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

        private async Task BlockingWriteToRedshiftAsync(CancellationToken token, string tableName, ChangeBatch batch)
        {
            var uploaded = false;
            while (!token.IsCancellationRequested && !uploaded)
            {
                uploaded = await WriteToRedshiftAsync(tableName, batch);
                if (!uploaded)
                {
                    Console.WriteLine($"Table {tableName} - Could not upload batch. Will retry in 10 seconds.");
                    await WaitForSeconds(token, 10);
                }
            }
        }

        private async Task<bool> WriteToRedshiftAsync(string tableName, ChangeBatch batch)
        {
            if (batch.Changes.Any())
            {
                var rowChanges = new List<RowChange>();
                foreach (var record in batch.Changes)
                {
                    rowChanges.Add(new RowChange()
                    {
                        ChangeKey = record.ChangeKey,
                        ChangeType = (CdcTools.Redshift.Changes.ChangeType)record.ChangeType,
                        Data = record.Data,
                        Lsn = record.LsnStr,
                        SeqVal = record.SeqValStr
                    });
                }

                try
                {
                    await _redshiftClient.UploadAsCsvAsync(tableName, rowChanges);
                    return true;
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"{tableName} upload failed. {ex}");
                    return false;
                }
            }

            return true;
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
