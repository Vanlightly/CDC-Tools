using CdcTools.CdcReader;
using CdcTools.CdcReader.Changes;
using CdcTools.CdcReader.State;
using CdcTools.CdcReader.Tables;
using CdcTools.Redshift;
using CdcTools.Redshift.Changes;
using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace CdcTools.CdcToRedshift
{
    public class FullLoadExporter
    {
        private List<Task> _loadTasks;
        private CdcReaderClient _cdcReaderClient;
        private RedshiftClient _redshiftClient;

        public FullLoadExporter(CdcReaderClient cdcReaderClient, RedshiftClient redshiftClient)
        {
            _cdcReaderClient = cdcReaderClient;
            _redshiftClient = redshiftClient;
            _loadTasks = new List<Task>();
        }

        public async Task ExportTablesAsync(CancellationToken token,
            string executionId,
            List<string> tables,
            int batchSize,
            int printMod)
        {
            await _redshiftClient.CacheTableColumnsAsync(tables);

            foreach (var table in tables)
            {
                var schemaName = table.Contains(".") ? table.Substring(0, table.IndexOf(".")) : "dbo";
                var tableName = table.Contains(".") ? table.Substring(table.IndexOf(".") + 1) : table;
                var tableSchema = await _cdcReaderClient.GetTableSchemaAsync(schemaName, tableName);
                _loadTasks.Add(Task.Run(async () =>
                {
                    try
                    {
                        await ExportTableAsync(token,
                            executionId,
                            tableSchema,
                            batchSize,
                            printMod);
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex);
                    }
                }));
            }
        }

        public void WaitForCompletion()
        {
            Task.WaitAll(_loadTasks.ToArray());
        }

        private async Task ExportTableAsync(CancellationToken token,
            string executionId,
            TableSchema tableSchema,
            int batchSize,
            int printPercentProgressMod)
        {
            var rowCount = await _cdcReaderClient.GetRowCountAsync(tableSchema);
            Console.WriteLine($"Table {tableSchema.TableName} - {rowCount} rows to export");
            int progress = 0;

            PrimaryKeyValue lastRetrievedKey = await SetStartingPosition(executionId, tableSchema, batchSize);
            long ctr = batchSize;
            bool finished = false;
            
            while (!token.IsCancellationRequested && !finished)
            {
                var changes = new List<RowChange>();

                var batch = await _cdcReaderClient.GetBatchAsync(tableSchema, lastRetrievedKey, batchSize);
                var result = await WriteToRedshiftAsync(batch, ctr);
                if (result.Item1)
                {
                    ctr = result.Item2;
                    int latestProgress = (int)(((double)ctr / (double)rowCount) * 100);
                    if (progress != latestProgress && latestProgress % printPercentProgressMod == 0)
                        Console.WriteLine($"Table {tableSchema.TableName} - Progress at {latestProgress}% ({ctr} records)");

                    progress = latestProgress;
                    lastRetrievedKey = batch.LastRowKey;
                    if(batch.Records.Any())
                        await _cdcReaderClient.StoreFullLoadOffsetAsync(executionId, tableSchema.TableName, lastRetrievedKey);

                    if (!batch.Records.Any() || batch.Records.Count < batchSize)
                        finished = true;
                }
                else
                {
                    Console.WriteLine($"Table {tableSchema.TableName} - Failed to upload to Redshift. Will try again in 10 seconds.");
                    await WaitForSeconds(token, 10);
                }
            }

            if (token.IsCancellationRequested)
                Console.WriteLine($"Table {tableSchema.Schema}.{tableSchema.TableName} - cancelled at progress at {progress}% ({ctr} records)");
            else
                Console.WriteLine($"Table {tableSchema.Schema}.{tableSchema.TableName} - complete ({ctr} records)");
        }

        private async Task<PrimaryKeyValue> SetStartingPosition(string executionId, TableSchema tableSchema, int batchSize)
        {
            PrimaryKeyValue lastRetrievedKey = null;
            long ctr = 0;
            var existingOffsetResult = await _cdcReaderClient.GetLastFullLoadOffsetAsync(executionId, tableSchema.TableName);
            if (existingOffsetResult.Result == Result.NoStoredState)
            {
                Console.WriteLine($"Table {tableSchema.TableName} - No previous stored offset. Starting from first row");
                var firstBatch = await _cdcReaderClient.GetFirstBatchAsync(tableSchema, batchSize);
                if (firstBatch.Records.Any())
                {
                    lastRetrievedKey = firstBatch.LastRowKey;
                    var result = await WriteToRedshiftAsync(firstBatch, ctr);
                    if (!result.Item1)
                    {
                        Console.WriteLine($"Table {tableSchema.TableName} - Export aborted");
                        return null;
                    }

                    await _cdcReaderClient.StoreFullLoadOffsetAsync(executionId, tableSchema.TableName, lastRetrievedKey);

                    ctr = result.Item2;
                    Console.WriteLine($"Table {tableSchema.TableName} - Written first batch to Redshift");
                }
                else
                {
                    Console.WriteLine($"Table {tableSchema.TableName} - No data to export");
                    return null;
                }
            }
            else
            {
                Console.WriteLine($"Table {tableSchema.TableName} - Starting from stored offset");
                lastRetrievedKey = existingOffsetResult.State;
            }

            return lastRetrievedKey;
        }

        private async Task<Tuple<bool, long>> WriteToRedshiftAsync(FullLoadBatch batch, long ctr)
        {
            if (batch.Records.Any())
            {
                var rowChanges = new List<RowChange>();
                foreach (var record in batch.Records)
                {
                    rowChanges.Add(new RowChange()
                    {
                        ChangeKey = record.ChangeKey,
                        ChangeType = CdcTools.Redshift.Changes.ChangeType.INSERT,
                        Data = record.Data,
                        Lsn = ctr.ToString(),
                        SeqVal = ctr.ToString()
                    });
                    ctr++;
                }

                try
                {
                    await _redshiftClient.UploadAsCsvAsync(batch.TableSchema.TableName, rowChanges);
                    return Tuple.Create(true, ctr);
                }
                catch(Exception ex)
                {
                    Console.WriteLine($"{batch.TableSchema.TableName} upload failed. {ex}");
                    return Tuple.Create(false, ctr);
                }
            }

            return Tuple.Create(true, ctr);
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
