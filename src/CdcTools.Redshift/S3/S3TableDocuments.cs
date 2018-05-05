using System;
using System.Collections.Generic;
using System.Text;

namespace CdcTools.Redshift.S3
{
    public class S3TableDocuments
    {
        public string Lsn { get; set; }
        public string TableName { get; set; }
        public string UpsertPath { get; set; }
        public string DeletePath { get; set; }
        public int Part { get; set; }
        public int PartCount { get; set; }
    }
}
