using CdcTools.CdcReader.Changes;
using CdcTools.CdcReader.State;
using CdcTools.CdcReader.Tables;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace CdcTools.CdcReader
{
    public class CdcReaderClient
    {
        private ICdcRepository _cdcRepository;
        private ITableSchemaRepository _tableSchemaRepository;
        private IFullLoadRepository _fullLoadRepository;
        private IStateManager _stateManager;

        public CdcReaderClient(string connectionString,
            string stateManagementConnectionString,
            ICdcRepository cdcRepository=null,
            ITableSchemaRepository tableSchemaRepository=null,
            IFullLoadRepository fullLoadRepository=null,
            IStateManager stateManager=null)
        {
            if (cdcRepository == null)
                _cdcRepository = new CdcRepository(connectionString);
            else
                _cdcRepository = cdcRepository;

            if (tableSchemaRepository == null)
                _tableSchemaRepository = new TableSchemaRepository(connectionString);
            else
                _tableSchemaRepository = tableSchemaRepository;

            if (fullLoadRepository == null)
                _fullLoadRepository = new FullLoadRepository(connectionString);
            else
                _fullLoadRepository = fullLoadRepository;

            if (stateManager == null)
                _stateManager = new StateManager(stateManagementConnectionString);
            else
                _stateManager = stateManager;
        }

        public async Task<byte[]> GetMinValidLsnAsync(string tableName)
        {
            return await _cdcRepository.GetMinValidLsnAsync(tableName);
        }

        public async Task<byte[]> GetMaxLsnAsync()
        {
            return await _cdcRepository.GetMaxLsnAsync();
        }

        public async Task<ChangeBatch> GetChangeBatchAsync(TableSchema tableSchema, byte[] fromLsn, byte[] fromSeqVal, byte[] toLsn, int batchSize)
        {
            return await _cdcRepository.GetChangeBatchAsync(tableSchema, fromLsn, fromSeqVal, toLsn, batchSize);
        }

        public async Task<ChangeBatch> GetChangeBatchAsync(TableSchema tableSchema, byte[] fromLsn, byte[] toLsn, int batchSize)
        {
            return await _cdcRepository.GetChangeBatchAsync(tableSchema, fromLsn, toLsn, batchSize);
        }

        public async Task<TableSchema> GetTableSchemaAsync(string schemaName, string tableName)
        {
            return await _tableSchemaRepository.GetTableSchemaAsync(schemaName, tableName);
        }

        public async Task<FullLoadBatch> GetFirstBatchAsync(TableSchema tableSchema, int batchSize)
        {
            return await _fullLoadRepository.GetFirstBatchAsync(tableSchema, batchSize);
        }

        public async Task<FullLoadBatch> GetBatchAsync(TableSchema tableSchema, PrimaryKeyValue lastRetrievedKey, int batchSize)
        {
            return await _fullLoadRepository.GetBatchAsync(tableSchema, lastRetrievedKey, batchSize);
        }

        public async Task<long> GetRowCountAsync(TableSchema tableSchema)
        {
            return await _fullLoadRepository.GetRowCountAsync(tableSchema);
        }

        public async Task<StateResult<Offset>> GetLastCdcOffsetAsync(string executionId, string tableName)
        {
            return await _stateManager.GetLastCdcOffsetAsync(executionId, tableName);
        }

        public async Task StoreCdcOffsetAsync(string executionId, string tableName, Offset offset)
        {
            await _stateManager.StoreCdcOffsetAsync(executionId, tableName, offset);
        }

        public async Task<StateResult<PrimaryKeyValue>> GetLastFullLoadOffsetAsync(string executionId, string tableName)
        {
            return await _stateManager.GetLastPkOffsetAsync(executionId, tableName);
        }

        public async Task StoreFullLoadOffsetAsync(string executionId, string tableName, PrimaryKeyValue pkValue)
        {
            await _stateManager.StorePkOffsetAsync(executionId, tableName, pkValue);
        }
    }
}
