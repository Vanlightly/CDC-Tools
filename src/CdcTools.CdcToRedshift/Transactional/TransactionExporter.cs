using CdcTools.CdcReader.Changes;
using CdcTools.CdcReader.Transactional;
using CdcTools.CdcReader.Transactional.State;
using CdcTools.Redshift;
using CdcTools.Redshift.Changes;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace CdcTools.CdcToRedshift.Transactional
{
    public class TransactionExporter
    {
        private Task _exporterTask;
        private CdcTransactionClient _cdcTransactionClient;
        private RedshiftClient _redshiftClient;

        public TransactionExporter(CdcTransactionClient cdcTransactionClient,
            RedshiftClient redshiftClient)
        {
            _cdcTransactionClient = cdcTransactionClient;
            _redshiftClient = redshiftClient;
        }

        public async Task StartExportingChangesAsync(CancellationToken token, 
            string executionId, 
            List<string> tables, 
            TimeSpan interval,
            int perTableBufferLimit,
            int transactionBufferLimit,
            int transactionBatchSizeLimit)
        {
            await _redshiftClient.CacheTableColumnsAsync(tables);

            _exporterTask = Task.Run(async () =>
                {
                    while (!token.IsCancellationRequested)
                    {
                        try
                        {
                            await StartExportingAsync(token,
                                executionId,
                                tables,
                                interval,
                                perTableBufferLimit,
                                transactionBufferLimit,
                                transactionBatchSizeLimit);
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"Transaction reader failure. Will restart in 30 seconds. Error: {ex}");
                            await WaitForSeconds(token, 30);
                        }
                    }
                });
        }

        public void WaitForCompletion()
        {
            _exporterTask.Wait();
        }

        private async Task StartExportingAsync(CancellationToken token,
            string executionId, 
            List<string> tables, 
            TimeSpan interval, 
            int perTableBufferLimit,
            int transactionBufferLimit,
            int transactionBatchSizeLimit)
        {
            var lastTran = await _cdcTransactionClient.GetLastTransactionIdAsync(executionId);
            if (lastTran.Result == Result.NoStoredTransationId)
                await _cdcTransactionClient.StartAsync(tables, perTableBufferLimit, transactionBufferLimit, transactionBatchSizeLimit);
            else
                await _cdcTransactionClient.StartAsync(tables, perTableBufferLimit, transactionBufferLimit, transactionBatchSizeLimit, lastTran.State.Lsn);

            string uncommittedLsn = string.Empty;
            bool haveUncommitedParts = false;

            while (!token.IsCancellationRequested)
            {
                var batches = new List<TransactionBatch>();
                var sw = new Stopwatch();
                sw.Start();

                while (sw.Elapsed <= interval && !token.IsCancellationRequested)
                {
                    var transactionBatch = await _cdcTransactionClient.NextAsync(token);
                    if (transactionBatch != null)
                    {
                        // if we have uncommitted multi-part transactions and the latest transaction is not the uncommitted one
                        // then we need to commit it now before continuing
                        if(haveUncommitedParts && !uncommittedLsn.Equals(transactionBatch.Id.LsnStr))
                        {
                            await _redshiftClient.CommitMultiplePartsAsync(uncommittedLsn);
                            haveUncommitedParts = false;
                            uncommittedLsn = string.Empty;
                        }

                        if (transactionBatch.IsMultiPart)
                        {
                            // if this is a multi-part transaction then trigger the upload of any accumulated transactions before starting to process the multi-part transaction
                            if(!haveUncommitedParts && batches.Any())
                            {
                                await UploadBatchesAsync(batches);
                                batches = new List<TransactionBatch>();
                                sw.Reset();
                                sw.Start();
                            }

                            foreach (var tableGroup in transactionBatch.Changes.GroupBy(x => x.TableName))
                            {
                                var orderedTableChanges = tableGroup.OrderBy(x => x.LsnInt).ThenBy(x => x.SeqValInt).ToList();
                                var rowChanges = ConvertToRowChanges(orderedTableChanges);
                                await _redshiftClient.StorePartAsCsvAsync(transactionBatch.Id.LsnStr, tableGroup.Key, transactionBatch.Part, rowChanges);
                            }

                            haveUncommitedParts = true;
                        }
                        else
                        {
                            batches.Add(transactionBatch);
                        }
                    }
                }

                if (batches.Any())
                {
                    // upload accumulated non multi-part batches to Redshift
                    Console.WriteLine($"Uploading {batches.Count} transactions with a total of {batches.SelectMany(x => x.Changes).Count()} changes");
                    await UploadBatchesAsync(batches);

                    // store our highest uploaded current transaction id in our state store
                    await _cdcTransactionClient.StoreTransactionIdAsync(executionId, batches.Last().Id);
                }
            }

            _cdcTransactionClient.Stop();
        }

        private async Task UploadBatchesAsync(List<TransactionBatch> batches)
        {
            var tableRowChanges = new Dictionary<string, List<RowChange>>();
            foreach(var batch in batches)
            {
                foreach(var tableGroup in batch.Changes.GroupBy(x => x.TableName))
                {
                    var orderedChanges = tableGroup.OrderBy(x => x.LsnInt).ThenBy(x => x.SeqValInt).ToList();
                    if (!tableRowChanges.ContainsKey(tableGroup.Key))
                        tableRowChanges.Add(tableGroup.Key, new List<RowChange>());

                    tableRowChanges[tableGroup.Key].AddRange(ConvertToRowChanges(orderedChanges));
                }
            }

            await _redshiftClient.UploadAsCsvAsync(tableRowChanges);
        }

        private List<RowChange> ConvertToRowChanges(List<ChangeRecord> changeRecords)
        {
            return changeRecords.Select(x => new RowChange()
            {
                ChangeKey = x.ChangeKey,
                ChangeType = (Redshift.Changes.ChangeType)x.ChangeType,
                Data = x.Data,
                Lsn = x.LsnStr,
                SeqVal = x.SeqValStr
            }).ToList();
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
