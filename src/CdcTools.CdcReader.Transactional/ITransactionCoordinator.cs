using CdcTools.CdcReader.Changes;
using CdcTools.CdcReader.Tables;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace CdcTools.CdcReader.Transactional
{
    public interface ITransactionCoordinator
    {
        Dictionary<string, BlockingCollection<ChangeRecord>> StartTableReaders(CancellationToken token,
            List<TableSchema> tableSchemas,
            int batchSize,
            byte[] lastRetrievedLsn);

        void StartGroupingTransactions(CancellationToken token,
            List<TableSchema> tableSchemas,
            Dictionary<string, BlockingCollection<ChangeRecord>> tableChangeBuffers,
            BlockingCollection<TransactionBatch> transactionBatchBuffer,
            int transactionBatchSizeLimit);

        bool IsCompleted();
    }
}
