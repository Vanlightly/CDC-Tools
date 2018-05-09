using CdcTools.CdcReader.Changes;
using CdcTools.CdcReader.Tables;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Numerics;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace CdcTools.CdcReader.Transactional
{
    public class TransactionCoordinator : ITransactionCoordinator
    {
        private List<Task> _tableReaderTasks;
        private Task _transactionGroupingTask;
        private ICdcRepository _cdcRepository;
        private BigInteger NoDataAvailable = 0;
                
        public TransactionCoordinator(ICdcRepository cdcRepository)
        {
            _cdcRepository = cdcRepository;
            _tableReaderTasks = new List<Task>();
        }

        public Dictionary<string, BlockingCollection<ChangeRecord>> StartTableReaders(CancellationToken token,
            List<TableSchema> tableSchemas,
            int batchSize,
            byte[] lastRetrievedLsn)
        {
            var tableChangeBuffers = new Dictionary<string, BlockingCollection<ChangeRecord>>();

            // start table CDC readers
            foreach (var tableSchema in tableSchemas)
            {
                var buffer = new BlockingCollection<ChangeRecord>();
                tableChangeBuffers.Add(tableSchema.TableName, buffer);

                _tableReaderTasks.Add(Task.Run(async () =>
                {
                    try
                    {
                        await ReadTransactionsAsync(token, tableSchema, batchSize, buffer, lastRetrievedLsn);
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Table {tableSchema.TableName} - Reader failure. {ex}");
                    }
                }));
            }

            return tableChangeBuffers;
        }

        public void StartGroupingTransactions(CancellationToken token, 
            List<TableSchema> tableSchemas,
            Dictionary<string, BlockingCollection<ChangeRecord>> tableChangeBuffers,
            BlockingCollection<TransactionBatch> transactionBatchBuffer,
            int transactionBatchSizeLimit)
        {
            _transactionGroupingTask = Task.Run(async () =>
            {
                try
                {
                    await GroupTransactionsAsync(token, tableSchemas, tableChangeBuffers, transactionBatchBuffer, transactionBatchSizeLimit);
                }
                catch(Exception ex)
                {
                    Console.WriteLine($"Transaction grouping failure. {ex}");
                }
            });
        }

        public bool IsCompleted()
        {
            if (_tableReaderTasks.All(x => x.IsCompleted))
                return _transactionGroupingTask.IsCompleted;

            return false;
        }

        private async Task GroupTransactionsAsync(CancellationToken token,
            List<TableSchema> tableSchemas,
            Dictionary<string, BlockingCollection<ChangeRecord>> tableChangeBuffers,
            BlockingCollection<TransactionBatch> transactionBatchBuffer,
            int transactionBatchSizeLimit)
        {
            // create a "one item per table" buffer
            var currentBuffer = new Dictionary<string, ChangeRecord>();
            foreach (var tableSchema in tableSchemas)
                currentBuffer.Add(tableSchema.TableName, null);

            while (!token.IsCancellationRequested)
            {
                // Pull in next values of any table we don't have values for yet or any who previously signalled they had no data at that time
                var keys = currentBuffer.Keys.ToList();
                foreach (var tableKey in keys)
                {
                    var bufferedValue = currentBuffer[tableKey];
                    if (bufferedValue == null || bufferedValue.LsnInt == NoDataAvailable)
                    {
                        ChangeRecord change = null;
                        if (tableChangeBuffers[tableKey].TryTake(out change))
                            currentBuffer[tableKey] = change;
                    }
                }

                if (currentBuffer.Values.All(x => x == null || x.LsnInt == NoDataAvailable))
                {
                    await Task.Delay(1000);
                    continue;
                }

                // identify the lowest LSN currently available in our current buffer and a create a new batch for it
                var currentTransaction = GetEarliestTransactionId(currentBuffer);
                var transactionBatch = new TransactionBatch();
                transactionBatch.Id = currentTransaction;
                transactionBatch.Part = 1;

                // Pull data from all table buffers for this LSN
                var matchingTables = GetTablesOfTransaction(currentBuffer, currentTransaction.LsnInt);
                foreach (var tableName in matchingTables)
                {
                    // add the first change of the transaction from our current buffer
                    transactionBatch.Changes.Add(currentBuffer[tableName]);

                    // keep consuming from this table buffer until either the LSN does not match our current LSN 
                    // or the table reader signals that we have reached the end of the buffer and no more items will arrive
                    var tableBuffer = tableChangeBuffers[tableName];
                    bool consume = true;
                    while (consume && !tableBuffer.IsAddingCompleted)
                    {
                        ChangeRecord next = null;
                        if (tableChangeBuffers[tableName].TryTake(out next))
                        {
                            if (next.LsnInt == currentTransaction.LsnInt)
                            {
                                transactionBatch.Changes.Add(next);

                                // if we have reached the maximum batch size the post and and create another
                                if (transactionBatch.Changes.Count > transactionBatchSizeLimit)
                                {
                                    await PostBatch(token, transactionBatchBuffer, transactionBatch);
                                    
                                    var nextPart = new TransactionBatch();
                                    nextPart.Part = transactionBatch.Part + 1;
                                    nextPart.Id = transactionBatch.Id;
                                    nextPart.IsMultiPart = true;

                                    transactionBatch = nextPart;
                                }
                            }
                            else
                            {
                                // store the value in our current buffer for a future transaction batch
                                currentBuffer[tableName] = next;
                                consume = false;
                            }
                        }
                        else
                        {
                            // there is no data in the buffer but the buffer is not completed. This means that the table reader is slower than us. Wait a little.
                            await Task.Delay(100);
                        }
                    }
                }

                // all data has been pulled from the buffers for this transaction
                // now we post it to the TransactionBatch buffer. 
                await PostBatch(token, transactionBatchBuffer, transactionBatch);
            }
        }

        private TransactionId GetEarliestTransactionId(Dictionary<string, ChangeRecord> currentValues)
        {
            TransactionId transactionId = null;
            foreach (var pair in currentValues)
            {
                if (transactionId == null)
                    transactionId = new TransactionId(pair.Value.Lsn, pair.Value.LsnStr, pair.Value.LsnInt);

                if(pair.Value.LsnInt < transactionId.LsnInt)
                    transactionId = new TransactionId(pair.Value.Lsn, pair.Value.LsnStr, pair.Value.LsnInt);
            }

            return transactionId;
        }

        private List<string> GetTablesOfTransaction(Dictionary<string, ChangeRecord> currentValues, BigInteger lsn)
        {
            var tables = new List<string>();

            foreach(var pair in currentValues)
            {
                if (pair.Value.LsnInt == lsn)
                    tables.Add(pair.Key);
            }

            return tables;
        }

        private async Task PostBatch(CancellationToken token, BlockingCollection<TransactionBatch> transactionBatchBuffer, TransactionBatch transactionBatch)
        {
            // If the buffer is full then wait until capacity is available
            // this ensures we have back pressure to prevent using up all our memory
            bool added = false;
            while (!added && !token.IsCancellationRequested)
            {
                if (transactionBatchBuffer.TryAdd(transactionBatch))
                    added = true;
                else
                    await Task.Delay(100);
            }
        }

        private async Task ReadTransactionsAsync(CancellationToken token, TableSchema tableSchema, int batchSize, BlockingCollection<ChangeRecord> changes, byte[] lastRetrievedLsn)
        {
            byte[] toLsn = await _cdcRepository.GetMaxLsnAsync();

            byte[] fromLsn = null;
            byte[] fromSeqVal = null;
            if (lastRetrievedLsn == null)
                fromLsn = await _cdcRepository.GetMinValidLsnAsync(tableSchema.TableName);
            else
                fromLsn = lastRetrievedLsn;

            while (!token.IsCancellationRequested)
            {
                bool unfinishedLsn = false;
                bool more = true;

                while (more && !token.IsCancellationRequested)
                {
                    if (GetBigInteger(fromLsn) <= GetBigInteger(toLsn))
                    {
                        ChangeBatch batch = null;
                        if (unfinishedLsn)
                            batch = await _cdcRepository.GetChangeBatchAsync(tableSchema, fromLsn, fromSeqVal, toLsn, batchSize);
                        else
                            batch = await _cdcRepository.GetChangeBatchAsync(tableSchema, fromLsn, toLsn, batchSize);

                        if (batch.Changes.Any())
                        {
                            //Console.WriteLine($"Table {tableName} - Retrieved block #{blockCounter} with {batch.Changes.Count} changes");
                            var enumerator = batch.Changes.GetEnumerator();
                            if (!enumerator.MoveNext())
                                break;

                            while (!token.IsCancellationRequested)
                            {
                                if (changes.TryAdd(enumerator.Current, 1000))
                                {
                                    if (!enumerator.MoveNext())
                                        break;
                                }
                            }

                            fromLsn = batch.Changes.Last().Lsn;
                            fromSeqVal = batch.Changes.Last().SeqVal;
                            more = batch.MoreChanges;
                            unfinishedLsn = batch.MoreOfLastTransaction;

                            if (unfinishedLsn)
                                fromSeqVal = Increment(fromSeqVal);
                            else
                                fromLsn = Increment(fromLsn);
                        }
                        else
                        {
                            more = false;
                            unfinishedLsn = false;
                            //Console.WriteLine($"Table {tableName} - No changes");
                        }
                    }
                    else
                    {
                        more = false;
                        unfinishedLsn = false;
                        //Console.WriteLine($"Table {tableName} - No changes");
                    }
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
