using CdcTools.CdcReader.Changes;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Numerics;
using System.Text;
using System.Threading.Tasks;

namespace CdcTools.CdcReader.Tables
{
    public class CdcRepository : ICdcRepository
    {
        private string _connString;
        private byte[] _noCdcDataLsn = new byte[10];

        public CdcRepository(string connectionString)
        {
            _connString = connectionString;
        }

        public async Task<byte[]> GetMinValidLsnAsync(string tableName)
        {
            using (var conn = await GetConnectionAsync())
            {
                var command = conn.CreateCommand();
                command.CommandText = string.Format(@"
DECLARE @from_lsn binary (10)
SET @from_lsn = sys.fn_cdc_get_min_lsn('{0}')
IF @from_lsn = 0x00000000000000000000
	SET @from_lsn = (SELECT TOP 1 __$start_lsn FROM [cdc].[dbo_{0}_CT] ORDER BY __$start_lsn)

SELECT @from_lsn", tableName);


                var result = await command.ExecuteScalarAsync();
                if (result != DBNull.Value)
                {
                    var lsn = (byte[])result;
                    return lsn;
                }
            }

            return new byte[10];
        }

        public async Task<byte[]> GetMaxLsnAsync()
        {
            using (var conn = await GetConnectionAsync())
            {
                var command = conn.CreateCommand();
                command.CommandText = "SELECT sys.fn_cdc_get_max_lsn()";
                var maxLsn = (byte[])await command.ExecuteScalarAsync();

                return maxLsn;
            }
        }

        public async Task<ChangeBatch> GetChangeBatchAsync(TableSchema tableSchema, byte[] fromLsn, byte[] fromSeqVal, byte[] toLsn, int batchSize)
        {
            var batch = new ChangeBatch();

            if (!HasValue(fromLsn))
                return batch;
            
            using (var conn = await GetConnectionAsync())
            {
                var command = conn.CreateCommand();
                command.Parameters.Add("@from_lsn", SqlDbType.Binary, 10).Value = fromLsn;
                command.Parameters.Add("@from_seqval", SqlDbType.Binary, 10).Value = fromSeqVal;
                command.Parameters.Add("@to_lsn", SqlDbType.Binary, 10).Value = toLsn;

                command.CommandText = string.Format(@"SELECT TOP {0} *
FROM [cdc].[fn_cdc_get_all_changes_dbo_{1}](@from_lsn, @to_lsn, 'all')
WHERE __$seqval >= @from_seqval
ORDER BY __$seqval", batchSize + 1, tableSchema.TableName);

                using (var reader = await command.ExecuteReaderAsync())
                {
                    int ctr = 0;
                    while (await reader.ReadAsync())
                    {
                        ctr++;
                        if (ctr <= batchSize)
                        {
                            var changeRecord = new ChangeRecord();
                            changeRecord.ChangeType = (ChangeType)(int)reader["__$operation"];
                            changeRecord.TableName = tableSchema.TableName;

                            changeRecord.Lsn = (byte[])reader["__$start_lsn"];
                            var lsn = new BigInteger(changeRecord.Lsn.Reverse().ToArray());
                            changeRecord.LsnStr = lsn.ToString();

                            changeRecord.SeqVal = (byte[])reader["__$seqval"];
                            var seqVal = new BigInteger(changeRecord.SeqVal.Reverse().ToArray());
                            changeRecord.SeqValStr = seqVal.ToString();

                            var recordIdSb = new StringBuilder();
                            foreach (var pkCol in tableSchema.PrimaryKeys.OrderBy(x => x.OrdinalPosition))
                            {
                                recordIdSb.Append(reader[pkCol.ColumnName].ToString());
                                recordIdSb.Append("|");
                            }
                            changeRecord.ChangeKey = recordIdSb.ToString();


                            foreach (var column in tableSchema.Columns)
                                changeRecord.Data.Add(column.Name.ToLower(), reader[column.Name]);

                            batch.Changes.Add(changeRecord);

                            if (ctr == 1)
                            {
                                batch.FromLsn = changeRecord.Lsn;
                                batch.FromSeqVal = changeRecord.SeqVal;
                            }
                            else
                            {
                                batch.ToLsn = changeRecord.Lsn;
                                batch.ToSeqVal = changeRecord.SeqVal;
                            }
                        }
                        else
                        {
                            batch.MoreChanges = true;

                            var lastLsn = new BigInteger(batch.ToLsn.Reverse().ToArray());
                            var lsn = new BigInteger(((byte[])reader["__$start_lsn"]).Reverse().ToArray());
                            if (lastLsn.Equals(lsn))
                                batch.MoreOfLastTransaction = true;
                        }
                    }
                }
            }

            return batch;
        }

        public async Task<ChangeBatch> GetChangeBatchAsync(TableSchema tableSchema, byte[] fromLsn, byte[] toLsn, int batchSize)
        {
            var fromStr = BitConverter.ToString(fromLsn);
            var toStr = BitConverter.ToString(toLsn);
            var batch = new ChangeBatch();

            if (!HasValue(fromLsn))
                return batch;

            using (var conn = await GetConnectionAsync())
            {
                var command = conn.CreateCommand();
                command.Parameters.Add("@from_lsn", SqlDbType.Binary, 10).Value = fromLsn;
                command.Parameters.Add("@to_lsn", SqlDbType.Binary, 10).Value = toLsn;

                command.CommandText = string.Format(@"SELECT TOP {0} *
FROM [cdc].[fn_cdc_get_all_changes_dbo_{1}](@from_lsn, @to_lsn, 'all')
ORDER BY __$seqval", batchSize + 1, tableSchema.TableName);

                using (var reader = await command.ExecuteReaderAsync())
                {
                    int ctr = 0;
                    while (await reader.ReadAsync())
                    {
                        ctr++;
                        if (ctr <= batchSize)
                        {
                            var changeRecord = new ChangeRecord();
                            changeRecord.ChangeType = (ChangeType)(int)reader["__$operation"];
                            changeRecord.TableName = tableSchema.TableName;

                            changeRecord.Lsn = (byte[])reader["__$start_lsn"];
                            var lsn = new BigInteger(changeRecord.Lsn.Reverse().ToArray());
                            changeRecord.LsnStr = lsn.ToString();

                            changeRecord.SeqVal = (byte[])reader["__$seqval"];
                            var seqVal = new BigInteger(changeRecord.SeqVal.Reverse().ToArray());
                            changeRecord.SeqValStr = seqVal.ToString();

                            var recordIdSb = new StringBuilder();
                            foreach (var pkCol in tableSchema.PrimaryKeys.OrderBy(x => x.OrdinalPosition))
                            {
                                recordIdSb.Append(reader[pkCol.ColumnName].ToString());
                                recordIdSb.Append("|");
                            }
                            changeRecord.ChangeKey = recordIdSb.ToString();


                            foreach (var column in tableSchema.Columns)
                                changeRecord.Data.Add(column.Name.ToLower(), reader[column.Name]);

                            batch.Changes.Add(changeRecord);

                            if (ctr == 1)
                            {
                                batch.FromLsn = changeRecord.Lsn;
                                batch.FromSeqVal = changeRecord.SeqVal;
                            }

                            if (ctr == batchSize) // not else if as could be batchSzie fo 1
                            {
                                batch.ToLsn = changeRecord.Lsn;
                                batch.ToSeqVal = changeRecord.SeqVal;
                            }
                        }
                        else
                        {
                            batch.MoreChanges = true;

                            var lastLsn = new BigInteger(batch.ToLsn.Reverse().ToArray());
                            var lsn = new BigInteger(((byte[])reader["__$start_lsn"]).Reverse().ToArray());
                            if (lastLsn.Equals(lsn))
                                batch.MoreOfLastTransaction = true;
                        }
                    }
                }
            }

            return batch;
        }

        private bool HasValue(byte[] lsn)
        {
            foreach(byte b in lsn)
            {
                if (b > 0)
                    return true;
            }

            return false;
        }

        private async Task<SqlConnection> GetConnectionAsync()
        {
            var conn = new SqlConnection(_connString);
            await conn.OpenAsync();

            return conn;
        }
    }
}
