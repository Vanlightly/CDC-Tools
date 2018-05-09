using System.Threading.Tasks;
using System.Data;
using System.Linq;
using System.Numerics;
using System.Collections.Generic;
using System.Data.SqlClient;

namespace CdcTools.CdcReader.Transactional.State
{
    public class StateManager : IStateManager
    {
        private string _connString;
        private byte[] _noCdcDataLsn = new byte[10];
        private HashSet<string> _cdcSeen;
        
        public StateManager(string connectionString)
        {
            _connString = connectionString;
            _cdcSeen = new HashSet<string>();
        }

        public async Task StoreTransactionIdAsync(string executionId, TransactionId transactionId)
        {
            if(_cdcSeen.Contains(executionId))
            {
                await UpdateStoreTransactionIdAsync(executionId, transactionId);
            }
            else
            {
                var result = await GetLastTransactionIdAsync(executionId);
                if(result.Result == Result.NoStoredTransationId)
                {
                    await InsertStoreTransactionIdAsync(executionId, transactionId);
                    _cdcSeen.Add(executionId);
                }
                else
                {
                    await UpdateStoreTransactionIdAsync(executionId, transactionId);
                }
            }
        }

        private async Task InsertStoreTransactionIdAsync(string executionId, TransactionId transactionId)
        {
            using (var conn = await GetConnectionAsync())
            {
                var command = conn.CreateCommand();
                command.CommandText = @"INSERT INTO [CdcTools].[TransactionState]([ExecutionId],[Lsn],[LastUpdate])
VALUES(@ExecutionId,@Lsn,GETUTCDATE())";
                command.Parameters.Add("ExecutionId", SqlDbType.VarChar, 50).Value = executionId;
                command.Parameters.Add("Lsn", SqlDbType.Binary, 10).Value = transactionId.Lsn;
                await command.ExecuteNonQueryAsync();
            }
        }

        private async Task UpdateStoreTransactionIdAsync(string executionId, TransactionId transactionId)
        {
            using (var conn = await GetConnectionAsync())
            {
                var command = conn.CreateCommand();
                command.CommandText = @"UPDATE [CdcTools].[TransactionState]
SET [Lsn] = @Lsn,
    [LastUpdate] = GETUTCDATE()
WHERE ExecutionId = @ExecutionId";
                command.Parameters.Add("ExecutionId", SqlDbType.VarChar, 50).Value = executionId;
                command.Parameters.Add("Lsn", SqlDbType.Binary, 10).Value = transactionId.Lsn;
                await command.ExecuteNonQueryAsync();
            }
        }

        public async Task<StateResult<TransactionId>> GetLastTransactionIdAsync(string executionId)
        {
            using (var conn = await GetConnectionAsync())
            {
                var command = conn.CreateCommand();
                command.CommandText = "SELECT TOP 1 Lsn FROM [CdcTools].[TransactionState] WHERE ExecutionId = @ExecutionId";
                command.Parameters.Add("ExecutionId", SqlDbType.VarChar, 50).Value = executionId;
                
                using (var reader = await command.ExecuteReaderAsync())
                {
                    if(reader.Read())
                    {
                        var lsn = (byte[])reader["Lsn"];
                        var lsnInt = new BigInteger(lsn.Reverse().ToArray());
                        var lsnStr = lsnInt.ToString();
                        var id = new TransactionId(lsn, lsnStr, lsnInt);

                        return new StateResult<TransactionId>(Result.TransactionIdReturned, id);
                    }
                    else
                    {
                        var lsn = new byte[10];
                        var lsnInt = 0;
                        var lsnStr = "0";
                        var id = new TransactionId(lsn, lsnStr, lsnInt);

                        return new StateResult<TransactionId>(Result.NoStoredTransationId, id);
                    }
                }
            }
        }

        private async Task<SqlConnection> GetConnectionAsync()
        {
            var conn = new SqlConnection(_connString);
            await conn.OpenAsync();

            return conn;
        }
    }
}
