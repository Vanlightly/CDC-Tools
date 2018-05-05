using System;
using System.Collections.Generic;
using System.Text;

namespace CdcTools.Redshift
{
    public class RedshiftConfiguration
    {
        public string Region { get; set; }
        public string Server { get; set; }
        public string Port { get; set; }
        public string MasterUsername { get; set; }
        public string MasterUserPassword { get; set; }
        public string DBName { get; set; }
        public string IamRole { get; set; }
        public string S3BucketName { get; set; }
        public string AccessKey { get; set; }
        public string SecretAccessKey { get; set; }
    }
}
