using System;
using System.Collections.Generic;
using System.Text;
using CdcTools.CdcReader.Changes;
using System.Data.SqlClient;
using System.Threading.Tasks;
using System.Linq;
using System.Data;

namespace CdcTools.CdcReader.Tables
{
    public class FullLoadRepository : IFullLoadRepository
    {
        private string _connString;

        public FullLoadRepository(string connectionString)
        {
            _connString = connectionString;
        }

        public async Task<long> GetRowCountAsync(TableSchema tableSchema)
        {
            using (var conn = await GetOpenConnectionAsync())
            {
                var command = conn.CreateCommand();
                command.CommandText = $"SELECT COUNT(*) FROM {tableSchema.Schema}.{tableSchema.TableName}";
                return (int) await command.ExecuteScalarAsync();
            }
        }

        public async Task<FullLoadBatch> GetFirstBatchAsync(TableSchema tableSchema, int batchSize)
        {
            var batch = new FullLoadBatch();
            batch.TableSchema = tableSchema;

            using (var conn = await GetOpenConnectionAsync())
            {
                var command = conn.CreateCommand();
                command.CommandText = $"SELECT TOP {batchSize} * FROM {tableSchema.Schema}.{tableSchema.TableName} ORDER BY {tableSchema.GetOrderedPrimaryKeyColumns()};";

                using (var reader = await command.ExecuteReaderAsync())
                {
                    int ctr = 1;
                    while (await reader.ReadAsync())
                    {
                        var change = new FullLoadRecord();
                        change.ChangeKey = GetRecordId(reader, tableSchema);
                        change.BatchSeqNo = ctr;

                        foreach (var column in tableSchema.Columns)
                            change.Data.Add(column.Name, reader[column.Name]);

                        batch.Records.Add(change);
                        ctr++;
                    }
                }
            }

            if (batch.Records.Any())
            {
                batch.FirstRowKey = GetKey(batch.Records.First(), tableSchema);
                batch.LastRowKey = GetKey(batch.Records.Last(), tableSchema);
            }

            return batch;
        }

        public async Task<FullLoadBatch> GetBatchAsync(TableSchema tableSchema, PrimaryKeyValue lastRetrievedKey, int batchSize)
        {
            var batch = new FullLoadBatch();
            batch.TableSchema = tableSchema;

            using (var conn = await GetOpenConnectionAsync())
            {
                var command = conn.CreateCommand();
                command.CommandText = TableSchemaQueryBuilder.GetExtractQueryUsingAllKeys(tableSchema, batchSize);
                
                foreach(var pk in tableSchema.PrimaryKeys.OrderBy(x => x.OrdinalPosition))
                {
                    var columnSchema = tableSchema.GetColumn(pk.ColumnName);
                    var value = lastRetrievedKey.GetValue(pk.OrdinalPosition);
                    command.Parameters.Add(CreateSqlParameter(columnSchema, "@p"+pk.OrdinalPosition, value));
                }

                using (var reader = await command.ExecuteReaderAsync())
                {
                    int ctr = 1;
                    while (await reader.ReadAsync())
                    {
                        var change = new FullLoadRecord();
                        change.ChangeKey = GetRecordId(reader, tableSchema);
                        change.BatchSeqNo = ctr;
                        
                        foreach (var column in tableSchema.Columns)
                            change.Data.Add(column.Name, reader[column.Name]);

                        batch.Records.Add(change);
                        ctr++;
                    }
                }
            }

            if (batch.Records.Any())
            {
                batch.FirstRowKey = GetKey(batch.Records.First(), tableSchema);
                batch.LastRowKey = GetKey(batch.Records.Last(), tableSchema);
            }

            return batch;
        }

        private PrimaryKeyValue GetKey(FullLoadRecord record, TableSchema tableSchema)
        {
            var pkVal = new PrimaryKeyValue();

            foreach (var pkCol in tableSchema.PrimaryKeys)
                pkVal.AddKeyValue(pkCol.OrdinalPosition, pkCol.ColumnName, record.Data[pkCol.ColumnName]);

            return pkVal;
        }

        private string GetRecordId(SqlDataReader reader, TableSchema tableSchema)
        {
            if (tableSchema.PrimaryKeys.Count == 1)
                return reader[tableSchema.PrimaryKeys.First().ColumnName].ToString();

            var recordIdSb = new StringBuilder();
            var pkCtr = 0;
            foreach (var pkCol in tableSchema.PrimaryKeys.OrderBy(x => x.OrdinalPosition))
            {
                if (pkCtr > 0)
                    recordIdSb.Append("|");

                recordIdSb.Append(reader[pkCol.ColumnName].ToString());
                pkCtr++;
            }

            return recordIdSb.ToString();
        }

        private async Task<SqlConnection> GetOpenConnectionAsync()
        {
            var conn = new SqlConnection(_connString);
            await conn.OpenAsync();

            return conn;
        }

        private SqlParameter CreateSqlParameter(ColumnSchema column, string parameterName, object value)
        {
            SqlParameter parameter = null;
            switch (column.DataType)
            {
                case "char":
                    parameter = new SqlParameter(parameterName, SqlDbType.Char, column.MaxCharsLength);
                    break;
                case "varchar":
                    parameter = new SqlParameter(parameterName, SqlDbType.VarChar, column.MaxCharsLength);
                    break;
                case "nvarchar":
                    parameter = new SqlParameter(parameterName, SqlDbType.NVarChar, column.MaxCharsLength);
                    break;
                case "tinyint":
                    parameter = new SqlParameter(parameterName, SqlDbType.TinyInt);
                    break;
                case "smallint":
                    parameter = new SqlParameter(parameterName, SqlDbType.SmallInt);
                    break;
                case "int":
                    parameter = new SqlParameter(parameterName, SqlDbType.Int);
                    break;
                case "bigint":
                    parameter = new SqlParameter(parameterName, SqlDbType.BigInt);
                    break;
                case "date":
                    parameter = new SqlParameter(parameterName, SqlDbType.Date);
                    break;
                case "datetime":
                    parameter = new SqlParameter(parameterName, SqlDbType.DateTime);
                    break;
                case "datetime2":
                    parameter = new SqlParameter(parameterName, SqlDbType.DateTime2);
                    break;
                case "time":
                    parameter = new SqlParameter(parameterName, SqlDbType.Time);
                    break;
                case "bit":
                    parameter = new SqlParameter(parameterName, SqlDbType.Bit);
                    break;
                case "money":
                    parameter = new SqlParameter(parameterName, SqlDbType.Money);
                    break;
                case "uniqueidentifier":
                    parameter = new SqlParameter(parameterName, SqlDbType.UniqueIdentifier);
                    break;
                case "varbinary":
                    parameter = new SqlParameter(parameterName, SqlDbType.VarBinary);
                    break;
                default:
                    throw new Exception("SQL data type not supported: " + column.DataType);
            }

            parameter.Value = value;

            return parameter;
        }
    }
}
