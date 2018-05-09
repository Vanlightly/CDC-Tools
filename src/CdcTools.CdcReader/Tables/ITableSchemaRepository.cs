using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace CdcTools.CdcReader.Tables
{
    public interface ITableSchemaRepository
    {
        Task<TableSchema> GetTableSchemaAsync(string tableName);
    }
}
