using CdcTools.Redshift.S3;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Odbc;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CdcTools.Redshift
{
    public class RedshiftDao : IRedshiftDao
    {
        #region Queries 
        private const string TablesQuery = @"SELECT ordinal_position,
         column_name,
         data_type,
         column_default,
         is_nullable,
         character_maximum_length,
         numeric_precision
    FROM information_schema.columns
   WHERE table_name = '[table]'
ORDER BY ordinal_position;";

        #endregion Queries 

        // table meta data
        private Dictionary<string, Dictionary<string, int>> _tableColumns;
        private Dictionary<string, string> _tablePkColumns;

        // for caching multi-part transactions
        // note that for reliability this would need to be persisted so that crashes and restarts would not lose this information
        private Dictionary<string, List<S3TableDocuments>> _pendingParts;
        private Dictionary<string, List<S3TableDocuments>> _pendingDeleteParts;

        // AWS config
        private RedshiftConfiguration _configuration;

        public RedshiftDao(RedshiftConfiguration configuration)
        {
            _tableColumns = new Dictionary<string, Dictionary<string, int>>();
            _tablePkColumns = new Dictionary<string, string>();
            _pendingParts = new Dictionary<string, List<S3TableDocuments>>();
            _pendingDeleteParts = new Dictionary<string, List<S3TableDocuments>>();

            _configuration = configuration;
        }

        public async Task PerformCsvMergeAsync(List<S3TableDocuments> tableUpdates)
        {
            OdbcConnection conn = null;

            try
            {
                conn = await GetOpenConnectionAsync();

                // create and fill staging tables
                var createStagingCommandSb = new StringBuilder();
                var docs = GetTableDocuments(tableUpdates);
                foreach (var tableUpdate in docs)
                {
                    createStagingCommandSb.AppendLine($"create temp table {tableUpdate.TableName.ToLower()}_stage_upsert (like {tableUpdate.TableName.ToLower()});");
                    createStagingCommandSb.AppendLine($"create temp table {tableUpdate.TableName.ToLower()}_stage_delete (like {tableUpdate.TableName.ToLower()});");

                    if (!string.IsNullOrEmpty(tableUpdate.UpsertPath))
                    {
                        createStagingCommandSb.AppendLine($"COPY {tableUpdate.TableName.ToLower()}_stage_upsert"
                                + $" FROM '{tableUpdate.UpsertPath}'"
                                + $" iam_role '{_configuration.IamRole}'"
                                + " delimiter '|'"
                                + $" region '{_configuration.Region}';");
                    }

                    if (!string.IsNullOrEmpty(tableUpdate.DeletePath))
                    {
                        createStagingCommandSb.AppendLine($"COPY {tableUpdate.TableName.ToLower()}_stage_delete"
                                + $" FROM '{tableUpdate.DeletePath}'"
                                + $" iam_role '{_configuration.IamRole}'"
                                + " delimiter '|'"
                                + $" region '{_configuration.Region}';");
                    }
                }

                var command = conn.CreateCommand();
                command.CommandText = createStagingCommandSb.ToString();
                await command.ExecuteNonQueryAsync();
                Console.WriteLine("Staging tables ready");

                // perform the merge
                var mergeSb = new StringBuilder();
                mergeSb.AppendLine("begin transaction;");
                foreach (var tableUpdate in tableUpdates)
                {
                    var rsTable = tableUpdate.TableName.ToLower(); // assumes same name but all lowercase - PoC!
                    mergeSb.AppendLine($"delete from {rsTable}");
                    mergeSb.AppendLine($"using {rsTable}_stage_upsert");
                    mergeSb.AppendLine($"where {rsTable}.{_tablePkColumns[rsTable]} = {rsTable}_stage_upsert.{_tablePkColumns[rsTable]};");
                    mergeSb.AppendLine("");

                    mergeSb.AppendLine($"delete from {rsTable}");
                    mergeSb.AppendLine($"using {rsTable}_stage_delete");
                    mergeSb.AppendLine($"where {rsTable}.{_tablePkColumns[rsTable]} = {rsTable}_stage_delete.{_tablePkColumns[rsTable]};");
                    mergeSb.AppendLine("");

                    mergeSb.AppendLine($"insert into {rsTable}");
                    mergeSb.AppendLine($"select * from {rsTable}_stage_upsert;");
                    mergeSb.AppendLine("");
                }

                mergeSb.AppendLine("end transaction;");

                command.CommandText = mergeSb.ToString();
                await command.ExecuteNonQueryAsync();
                Console.WriteLine("Merge complete");
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine("Merge failed: " + ex.Message);
            }
            finally
            {
                if (conn != null)
                    conn.Close();
            }
        }

        public async Task LoadTableColumnsAsync(List<string> tables)
        {
            foreach (var table in tables)
            {
                var columns = await GetTableColumnsAsync(table);
                _tableColumns.Add(table.ToLower(), columns);
            }
        }

        public List<string> GetOrderedColumns(string tableName)
        {
            var columns = _tableColumns[tableName];
            return columns.OrderBy(x => x.Value).Select(x => x.Key).ToList();
        }

        private async Task<Dictionary<string, int>> GetTableColumnsAsync(string tableName)
        {
            tableName = tableName.ToLower();
            var columns = new Dictionary<string, int>();
            DataSet ds = new DataSet();
            DataTable dt = new DataTable();

            string query = TablesQuery.Replace("[table]", tableName);

            try
            {
                var conn = await GetOpenConnectionAsync();
                OdbcDataAdapter da = new OdbcDataAdapter(query, conn);
                da.Fill(ds);
                dt = ds.Tables[0];
                foreach (DataRow row in dt.Rows)
                {
                    string columnName = row["column_name"].ToString();
                    int ordinalPosition = int.Parse(row["ordinal_position"].ToString());
                    columns.Add(columnName, ordinalPosition);

                    // big assumption here - this is a PoC only!
                    if (ordinalPosition == 1)
                        _tablePkColumns.Add(tableName, columnName);
                }

                conn.Close();
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine(ex.Message);
            }

            return columns;
        }

        private async Task<OdbcConnection> GetOpenConnectionAsync()
        {
            string connString = "Driver={Amazon Redshift (x64)};" +
                    String.Format("Server={0};Database={1};" +
                    "UID={2};PWD={3};Port={4};SSL=true;Sslmode=Require",
                    _configuration.Server, _configuration.DBName, _configuration.MasterUsername,
                    _configuration.MasterUserPassword, _configuration.Port);

            OdbcConnection conn = new OdbcConnection(connString);
            await conn.OpenAsync();

            return conn;
        }

        private List<S3TableDocuments> GetTableDocuments(List<S3TableDocuments> tableUpdates)
        {
            var docs = new List<S3TableDocuments>();

            foreach (var tableUpdate in tableUpdates)
            {
                if (tableUpdate.PartCount > 1) // if multi-part
                {
                    if (tableUpdate.Part == tableUpdate.PartCount) // if the last part
                    {
                        List<S3TableDocuments> cachedS3MetaData = null;
                        if (_pendingParts.TryGetValue(tableUpdate.Lsn, out cachedS3MetaData))
                        {
                            docs.AddRange(cachedS3MetaData);
                            _pendingParts.Remove(tableUpdate.Lsn);
                        }

                        docs.Add(tableUpdate);
                    }
                    else
                    {
                        // cache this part until we receive the last part and can upload them together
                        if (_pendingParts.ContainsKey(tableUpdate.Lsn))
                            _pendingParts[tableUpdate.Lsn].Add(tableUpdate);
                        else
                            _pendingParts.Add(tableUpdate.Lsn, new List<S3TableDocuments>() { tableUpdate });
                    }
                }
                else // is not multi-part so add it now
                {
                    docs.Add(tableUpdate);
                }
            }

            return docs;
        }
    }
}
