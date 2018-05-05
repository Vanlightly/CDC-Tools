using CdcTools.CdcReader.Changes;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace CdcTools.CdcReader.Tables
{
    public interface ICdcRepository
    {
        Task<byte[]> GetMinValidLsnAsync(string tableName);
        Task<byte[]> GetMaxLsnAsync();
        Task<ChangeBatch> GetChangeBatchAsync(TableSchema tableSchema, byte[] fromLsn, byte[] fromSeqVal, byte[] toLsn, int batchSize);
        Task<ChangeBatch> GetChangeBatchAsync(TableSchema tableSchema, byte[] fromLsn, byte[] toLsn, int batchSize);
    }
}
