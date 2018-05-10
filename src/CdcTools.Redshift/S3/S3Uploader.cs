using Amazon.S3;
using Amazon.S3.Model;
using CdcTools.Redshift.Changes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CdcTools.Redshift.S3
{
    public class S3Uploader : IS3Uploader
    {
        private string _bucketName;

        public S3Uploader(string bucketName)
        {
            _bucketName = bucketName;
        }

        public async Task<string> PutS3UpsertAsync(AmazonS3Client s3Client, string table, List<RowChange> changeRecords, List<string> orderedCols)
        {
            var changesToPut = GetValidChanges(changeRecords, ChangeType.INSERT, ChangeType.UPDATE_AFTER);
            if (!changesToPut.Any())
                return "";

            var document = BuildDocument(changesToPut, orderedCols);
            var s3Path = await PerformRequestAsync(s3Client, table, "upsert", document, changesToPut);

            Console.WriteLine($"Uploaded upsert to {s3Path} with {changesToPut.Count()} changes. {changeRecords.Count(x => x.ChangeType == ChangeType.INSERT || x.ChangeType == ChangeType.UPDATE_AFTER) - changesToPut.Count} redundant changes were omitted.");

            return s3Path;
        }

        public async Task<string> PutS3DeleteAsync(AmazonS3Client s3Client, string table, List<RowChange> changeRecords, List<string> orderedCols)
        {
            // where the last change to happen for a given record was a delete
            var changesToPut = GetValidChanges(changeRecords, ChangeType.DELETE);
            if (!changesToPut.Any())
                return "";

            var document = BuildDocument(changesToPut, orderedCols);
            var s3Path = await PerformRequestAsync(s3Client, table, "delete", document, changesToPut);

            Console.WriteLine($"Uploaded delete to {s3Path} with {changesToPut.Count()} changes. {changeRecords.Count(x => x.ChangeType == ChangeType.DELETE) - changesToPut.Count} redundant changes were omitted.");

            return s3Path;
        }

        public async Task<string> PutS3UpsertPartAsync(AmazonS3Client s3Client, string table, List<RowChange> changeRecords, List<string> orderedCols, int part)
        {
            var changesToPut = GetValidChanges(changeRecords, ChangeType.INSERT, ChangeType.UPDATE_AFTER);
            if (!changesToPut.Any())
                return "";

            var document = BuildDocument(changesToPut, orderedCols);
            var s3Path = await PerformRequestAsync(s3Client, table, "upsert", document, changesToPut, "_Part" + part.ToString().PadLeft(5, '0'));

            Console.WriteLine($"Uploaded upsert to {s3Path} with {changesToPut.Count()} changes. {changeRecords.Count(x => x.ChangeType == ChangeType.INSERT || x.ChangeType == ChangeType.UPDATE_AFTER) - changesToPut.Count} redundant changes were omitted.");

            return s3Path;
        }

        public async Task<string> PutS3DeletePartAsync(AmazonS3Client s3Client, string table, List<RowChange> changeRecords, List<string> orderedCols, int part)
        {
            // where the last change to happen for a given record was a delete
            var changesToPut = GetValidChanges(changeRecords, ChangeType.DELETE);
            if (!changesToPut.Any())
                return "";

            var document = BuildDocument(changesToPut, orderedCols);
            var s3Path = await PerformRequestAsync(s3Client, table, "delete", document, changesToPut, "_Part" + part.ToString().PadLeft(5, '0'));

            Console.WriteLine($"Uploaded upsert to {s3Path} with {changesToPut.Count()} changes. {changeRecords.Count(x => x.ChangeType == ChangeType.DELETE) - changesToPut.Count} redundant changes were omitted.");

            return s3Path;
        }

        private string BuildDocument(List<RowChange> changesToPut, List<string> orderedCols)
        {
            int count = changesToPut.Count;
            int ctr = 0;
            var sb = new StringBuilder();
            foreach (var change in changesToPut)
            {
                ctr++;
                for (int i = 0; i < orderedCols.Count; i++)
                {
                    object value = GetValue(orderedCols[i], change.Data);
                    if (value is DateTime)
                    {
                        DateTime dt = (DateTime)value;
                        sb.Append(dt.ToString("yyyy-MM-dd"));
                    }
                    else
                        sb.Append(value.ToString());

                    if (i < orderedCols.Count - 1)
                        sb.Append("|");
                }

                if (ctr < count)
                    sb.AppendLine("");
            }

            return sb.ToString();
        }

        private object GetValue(string column, Dictionary<string, object> data)
        {
            foreach(var pair in data)
            {
                if (pair.Key.Equals(column, StringComparison.OrdinalIgnoreCase))
                    return pair.Value;
            }

            return "";
        }

        private List<RowChange> GetValidChanges(List<RowChange> changeRecords, params ChangeType[] changeTypes)
        {
            var changesToPut = new List<RowChange>();

            var groupedByRecordId = changeRecords.GroupBy(x => x.ChangeKey).ToList();

            foreach (var changesOfRecord in groupedByRecordId)
            {
                var orderedChanges = changesOfRecord.OrderBy(x => x.LsnInteger).ThenBy(x => x.SeqValInteger).ToList();
                var lastChange = orderedChanges.Last();

                if (changeTypes.Contains(lastChange.ChangeType))
                    changesToPut.Add(lastChange);
            }
            
            return changesToPut;
        }

        private async Task<string> PerformRequestAsync(AmazonS3Client s3Client, string table, string changeType, string document, List<RowChange> changesToPut, string suffix="")
        {
            var request = new PutObjectRequest()
            {
                BucketName = _bucketName,
                ContentBody = document,
                Key = $"{table}/{changeType}/{changesToPut.Min(x => x.Lsn).ToString()}{suffix}",
                ContentType = "text/plain"
            };
            var response = await s3Client.PutObjectAsync(request);

            if (response.HttpStatusCode != System.Net.HttpStatusCode.OK)
            {
                // should check response and act in case of failure
                // this would need careful analysis of correct behaviour
                Console.WriteLine("Upload failure!");
                return "";
            }

            return $"s3://{request.BucketName}/{request.Key}";
        }
    }
}
