using Amazon;
using Amazon.S3;
using CdcTools.Redshift.Changes;
using CdcTools.Redshift.S3;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace CdcTools.Redshift
{
    public class RedshiftClient
    {
        private RedshiftConfiguration _configuration;
        private IRedshiftDao _redshiftDao;
        private IS3Uploader _s3Uploader;

        public RedshiftClient(RedshiftConfiguration configuration,
            IRedshiftDao redshiftDao = null,
            IS3Uploader s3Uploader = null)
        {
            _configuration = configuration;

            if (redshiftDao == null)
                _redshiftDao = new RedshiftDao(configuration);
            else
                _redshiftDao = redshiftDao;

            if (s3Uploader == null)
                _s3Uploader = new S3Uploader(configuration.S3BucketName);
            else
                _s3Uploader = s3Uploader;
        }

        public async Task CacheTableColumnsAsync(List<string> tableNames)
        {
            await _redshiftDao.LoadTableColumnsAsync(tableNames);
        }

        public async Task UploadAsCsvAsync(string tableName, List<RowChange> rowChanges)
        {
            tableName = tableName.ToLower();
            var s3TableDocs = await LoadToS3Async(tableName, rowChanges);
            await _redshiftDao.PerformCsvMergeAsync(s3TableDocs);
        }

        private async Task<List<S3TableDocuments>> LoadToS3Async(string tableName, List<RowChange> changesToPut)
        {
            var tableUpdates = new List<S3TableDocuments>();

            using (AmazonS3Client s3Client = GetS3Client())
            {
                var orderedColumns = _redshiftDao.GetOrderedColumns(tableName);
                var upsertPath = await _s3Uploader.PutS3UpsertAsync(s3Client, tableName, changesToPut, orderedColumns);
                var deletePath = await _s3Uploader.PutS3DeleteAsync(s3Client, tableName, changesToPut, orderedColumns);

                tableUpdates.Add(new S3TableDocuments() { TableName = tableName, UpsertPath = upsertPath, DeletePath = deletePath });

                return tableUpdates;
            }
        }

        private AmazonS3Client GetS3Client()
        {
            return new AmazonS3Client(_configuration.AccessKey,
                _configuration.SecretAccessKey,
                RegionEndpoint.GetBySystemName(_configuration.Region));
        }
    }
}
