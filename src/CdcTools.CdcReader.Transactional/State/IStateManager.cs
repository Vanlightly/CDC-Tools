using System.Threading.Tasks;

namespace CdcTools.CdcReader.Transactional.State
{
    public interface IStateManager
    {
        Task StoreTransactionIdAsync(string executionId, TransactionId transactionId);
        Task<StateResult<TransactionId>> GetLastTransactionIdAsync(string executionId);
    }
}
