using Amazon.S3;
using CdcTools.Redshift.Changes;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace CdcTools.Redshift.S3
{
    public interface IS3Uploader
    {
        Task<string> PutS3UpsertAsync(AmazonS3Client s3Client, string table, List<RowChange> changeRecords, List<string> orderedCols);
        Task<string> PutS3DeleteAsync(AmazonS3Client s3Client, string table, List<RowChange> changeRecords, List<string> orderedCols);
    }
}
