using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using CdcTools.CdcReader.Changes;
using CdcTools.CdcReader.Tables;
using System.Data.SqlClient;
using System.Data;
using Newtonsoft.Json;
using System.Collections.Concurrent;

namespace CdcTools.CdcReader.State
{
    public class StateManager : IStateManager
    {
        private string _connString;
        private byte[] _noCdcDataLsn = new byte[10];
        private ConcurrentDictionary<Tuple<string, string>, bool> _cdcSeen;
        private ConcurrentDictionary<Tuple<string, string>, bool> _fullLoadSeen;

        public StateManager(string connectionString)
        {
            _connString = connectionString;
            _cdcSeen = new ConcurrentDictionary<Tuple<string, string>, bool>();
            _fullLoadSeen = new ConcurrentDictionary<Tuple<string, string>, bool>();
        }

        public async Task StoreCdcOffsetAsync(string executionId, string tableName, Offset offset)
        {
            if(_cdcSeen.ContainsKey(Tuple.Create(executionId, tableName)))
            {
                await UpdateStoreCdcOffsetAsync(executionId, tableName, offset);
            }
            else
            {
                var result = await GetLastCdcOffsetAsync(executionId, tableName);
                if(result.Result == Result.NoStoredState)
                {
                    await InsertStoreCdcOffsetAsync(executionId, tableName, offset);
                    _cdcSeen.TryAdd(Tuple.Create(executionId, tableName), true);
                }
                else
                {
                    await UpdateStoreCdcOffsetAsync(executionId, tableName, offset);
                }
            }
        }

        private async Task InsertStoreCdcOffsetAsync(string executionId, string tableName, Offset offset)
        {
            using (var conn = await GetConnectionAsync())
            {
                var command = conn.CreateCommand();
                command.CommandText = @"INSERT INTO [CdcTools].[ChangeState]([ExecutionId],[TableName],[Lsn],[SeqVal],[LastUpdate])
VALUES(@ExecutionId,@TableName,@Lsn,@SeqVal,GETUTCDATE())";
                command.Parameters.Add("ExecutionId", SqlDbType.VarChar, 50).Value = executionId;
                command.Parameters.Add("TableName", SqlDbType.VarChar, 200).Value = tableName;
                command.Parameters.Add("Lsn", SqlDbType.Binary, 10).Value = offset.Lsn;
                command.Parameters.Add("SeqVal", SqlDbType.Binary, 10).Value = offset.SeqVal;
                await command.ExecuteNonQueryAsync();
            }
        }

        private async Task UpdateStoreCdcOffsetAsync(string executionId, string tableName, Offset offset)
        {
            using (var conn = await GetConnectionAsync())
            {
                var command = conn.CreateCommand();
                command.CommandText = @"UPDATE [CdcTools].[ChangeState]
SET [Lsn] = @Lsn,
    [SeqVal] = @SeqVal,
    [LastUpdate] = GETUTCDATE()
WHERE ExecutionId = @ExecutionId
AND TableName = @TableName";
                command.Parameters.Add("ExecutionId", SqlDbType.VarChar, 50).Value = executionId;
                command.Parameters.Add("TableName", SqlDbType.VarChar, 200).Value = tableName;
                command.Parameters.Add("Lsn", SqlDbType.Binary, 10).Value = offset.Lsn;
                command.Parameters.Add("SeqVal", SqlDbType.Binary, 10).Value = offset.SeqVal;
                await command.ExecuteNonQueryAsync();
            }
        }

        public async Task<StateResult<Offset>> GetLastCdcOffsetAsync(string executionId, string tableName)
        {
            using (var conn = await GetConnectionAsync())
            {
                var command = conn.CreateCommand();
                command.CommandText = "SELECT TOP 1 Lsn, SeqVal FROM [CdcTools].[ChangeState] WHERE ExecutionId = @ExecutionId AND TableName = @TableName";
                command.Parameters.Add("ExecutionId", SqlDbType.VarChar, 50).Value = executionId;
                command.Parameters.Add("TableName", SqlDbType.VarChar, 200).Value = tableName;

                using (var reader = await command.ExecuteReaderAsync())
                {
                    if(reader.Read())
                    {
                        var offset = new Offset();
                        offset.Lsn = (byte[])reader["Lsn"];
                        offset.SeqVal = (byte[])reader["SeqVal"];

                        return new StateResult<Offset>(Result.StateReturned, offset);
                    }
                    else
                    {
                        var offset = new Offset();
                        offset.Lsn = new byte[10];
                        offset.SeqVal = new byte[10];

                        return new StateResult<Offset>(Result.NoStoredState, offset);
                    }
                }
            }
        }

        public async Task<StateResult<PrimaryKeyValue>> GetLastPkOffsetAsync(string executionId, string tableName)
        {
            using (var conn = await GetConnectionAsync())
            {
                var command = conn.CreateCommand();
                command.CommandText = "SELECT TOP 1 PrimaryKeyValue FROM [CdcTools].[FullLoadState] WHERE ExecutionId = @ExecutionId AND TableName = @TableName";
                command.Parameters.Add("ExecutionId", SqlDbType.VarChar, 50).Value = executionId;
                command.Parameters.Add("TableName", SqlDbType.VarChar, 200).Value = tableName;

                using (var reader = await command.ExecuteReaderAsync())
                {
                    if (reader.Read())
                    {
                        var json = reader["PrimaryKeyValue"].ToString();
                        var pkValue = JsonConvert.DeserializeObject<PrimaryKeyValue>(json);
                        return new StateResult<PrimaryKeyValue>(Result.StateReturned, pkValue);
                    }
                    else
                    {
                        return new StateResult<PrimaryKeyValue>(Result.NoStoredState, null);
                    }
                }
            }
        }

        public async Task StorePkOffsetAsync(string executionId, string tableName, PrimaryKeyValue pkValue)
        {
            if (_fullLoadSeen.ContainsKey(Tuple.Create(executionId, tableName)))
            {
                await UpdateStorePkOffsetAsync(executionId, tableName, pkValue);
            }
            else
            {
                var result = await GetLastPkOffsetAsync(executionId, tableName);
                if (result.Result == Result.NoStoredState)
                {
                    await InsertStorePkOffsetAsync(executionId, tableName, pkValue);
                    _fullLoadSeen.TryAdd(Tuple.Create(executionId, tableName), true);
                }
                else
                {
                    await UpdateStorePkOffsetAsync(executionId, tableName, pkValue);
                }
            }
        }

        private async Task InsertStorePkOffsetAsync(string executionId, string tableName, PrimaryKeyValue pkValue)
        {
            using (var conn = await GetConnectionAsync())
            {
                var command = conn.CreateCommand();
                command.CommandText = @"INSERT INTO [CdcTools].[FullLoadState]([ExecutionId],[TableName],[PrimaryKeyValue],[LastUpdate])
VALUES(@ExecutionId,@TableName,@PrimaryKeyValue,GETUTCDATE())";
                command.Parameters.Add("ExecutionId", SqlDbType.VarChar, 50).Value = executionId;
                command.Parameters.Add("TableName", SqlDbType.VarChar, 200).Value = tableName;
                command.Parameters.Add("PrimaryKeyValue", SqlDbType.VarChar, -1).Value = JsonConvert.SerializeObject(pkValue);
                await command.ExecuteNonQueryAsync();
            }
        }

        private async Task UpdateStorePkOffsetAsync(string executionId, string tableName, PrimaryKeyValue pkValue)
        {
            using (var conn = await GetConnectionAsync())
            {
                var command = conn.CreateCommand();
                command.CommandText = @"UPDATE [CdcTools].[FullLoadState]
SET [PrimaryKeyValue] = @PrimaryKeyValue,
    [LastUpdate] = GETUTCDATE()
WHERE ExecutionId = @ExecutionId
AND TableName = @TableName";
                command.Parameters.Add("ExecutionId", SqlDbType.VarChar, 50).Value = executionId;
                command.Parameters.Add("TableName", SqlDbType.VarChar, 200).Value = tableName;
                command.Parameters.Add("PrimaryKeyValue", SqlDbType.VarChar, -1).Value = JsonConvert.SerializeObject(pkValue);
                await command.ExecuteNonQueryAsync();
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
