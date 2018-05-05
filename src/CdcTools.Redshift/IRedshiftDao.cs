using CdcTools.Redshift.S3;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace CdcTools.Redshift
{
    public interface IRedshiftDao
    {
        Task PerformCsvMergeAsync(List<S3TableDocuments> tableUpdates);
        Task LoadTableColumnsAsync(List<string> tables);
        List<string> GetOrderedColumns(string tableName);
    }
}
