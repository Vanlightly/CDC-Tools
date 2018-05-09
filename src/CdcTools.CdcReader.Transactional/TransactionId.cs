using System;
using System.Collections.Generic;
using System.Numerics;
using System.Text;

namespace CdcTools.CdcReader.Transactional
{
    public class TransactionId
    {
        public TransactionId(byte[] lsn, string lsnStr, BigInteger lsnInt)
        {
            Lsn = lsn;
            LsnStr = lsnStr;
            LsnInt = lsnInt;
        }

        public byte[] Lsn { get; set; }
        public string LsnStr { get; set; }
        public BigInteger LsnInt { get; set; }

        public override bool Equals(object obj)
        {
            var tranId = obj as TransactionId;
            if (tranId == null || LsnStr == null)
                return false;

            if (LsnStr == null)
                return false;

            return LsnStr.Equals(tranId.LsnStr, StringComparison.Ordinal);
        }

        public override int GetHashCode()
        {
            if (LsnStr == null)
                return 0;

            return LsnStr.GetHashCode();
        }
    }
}
