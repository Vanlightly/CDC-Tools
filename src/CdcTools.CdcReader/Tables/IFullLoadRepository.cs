using CdcTools.CdcReader.Changes;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace CdcTools.CdcReader.Tables
{
    public interface IFullLoadRepository
    {
        Task<long> GetRowCountAsync(TableSchema tableSchema);
        Task<FullLoadBatch> GetFirstBatchAsync(TableSchema tableSchema, int batchSize);
        Task<FullLoadBatch> GetBatchAsync(TableSchema tableSchema, PrimaryKeyValue lastRetrievedKey, int batchSize);
    }
}
