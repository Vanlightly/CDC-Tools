using CdcTools.CdcReader.Tables;
using CdcTools.CdcReader.Transactional.State;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace CdcTools.CdcReader.Transactional
{
    public class CdcTransactionClient
    {
        private ICdcRepository _cdcRepository;
        private ITableSchemaRepository _tableSchemaRepository;
        private IFullLoadRepository _fullLoadRepository;
        private IStateManager _stateManager;
        private ITransactionCoordinator _transactionCoordinator;
        private CancellationTokenSource _transactionCts;

        private BlockingCollection<TransactionBatch> _transactionBatchBuffer;
        private object _transactionLockObj = new object();

        public CdcTransactionClient(string connectionString,
            string stateManagementConnectionString,
            ICdcRepository cdcRepository=null,
            ITableSchemaRepository tableSchemaRepository=null,
            IFullLoadRepository fullLoadRepository=null,
            IStateManager stateManager=null,
            ITransactionCoordinator transactionCoordinator=null)
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

            if (transactionCoordinator == null)
                _transactionCoordinator = new TransactionCoordinator(_cdcRepository);
            else
                _transactionCoordinator = transactionCoordinator;

        }

        public async Task StartAsync(List<string> tables,
            int perTableBufferLength,
            int transactionBufferLength,
            int transactionBatchSizeLimit)
        {
            await StartAsync(tables, perTableBufferLength, transactionBufferLength, transactionBatchSizeLimit, null);
        }

        public async Task StartAsync(List<string> tables, 
            int perTableBufferLength,
            int transactionBufferLength,
            int transactionBatchSizeLimit,
            byte[] lastRetrievedLsn)
        {
            if (_transactionBatchBuffer != null)
                throw new InvalidOperationException("There is an active transaction stream that must be stopped first");

            var tableSchemas = new List<TableSchema>();
            foreach (var tableName in tables)
                tableSchemas.Add(await _tableSchemaRepository.GetTableSchemaAsync(tableName));

            lock (_transactionLockObj)
            {
                _transactionCts = new CancellationTokenSource();
                _transactionBatchBuffer = new BlockingCollection<TransactionBatch>(transactionBufferLength);
                var tableChangeBuffers = _transactionCoordinator.StartTableReaders(_transactionCts.Token, tableSchemas, perTableBufferLength, lastRetrievedLsn);
                _transactionCoordinator.StartGroupingTransactions(_transactionCts.Token, tableSchemas, tableChangeBuffers, _transactionBatchBuffer, transactionBatchSizeLimit);
            }
        }

        public async Task<TransactionBatch> NextAsync()
        {
            TransactionBatch batch = null;

            while(!_transactionBatchBuffer.TryTake(out batch))
                await Task.Delay(100);

            return batch;
        }

        public async Task<TransactionBatch> NextAsync(CancellationToken token)
        {
            TransactionBatch batch = null;

            while (!_transactionBatchBuffer.TryTake(out batch) && !token.IsCancellationRequested)
                await Task.Delay(100);

            return batch;
        }

        public async Task<TransactionBatch> NextAsync(TimeSpan waitPeriod)
        {
            var sw = new Stopwatch();
            sw.Start();
            TransactionBatch batch = null;

            while (!_transactionBatchBuffer.TryTake(out batch) && sw.Elapsed <= waitPeriod)
                await Task.Delay(100);

            return batch;
        }

        public void Stop()
        {
            lock (_transactionLockObj)
            {
                _transactionCts.Cancel();

                if (_transactionBatchBuffer != null)
                    _transactionBatchBuffer = null;
            }
        }

        public async Task<StateResult<TransactionId>> GetLastTransactionIdAsync(string executionId)
        {
            return await _stateManager.GetLastTransactionIdAsync(executionId);
        }

        public async Task StoreTransactionIdAsync(string executionId, TransactionId transactionId)
        {
            await _stateManager.StoreTransactionIdAsync(executionId, transactionId);
        }
    }
}
