using System;
using System.Collections.Generic;
using System.Text;

namespace CdcTools.CdcToKafka
{
    public enum RunMode
    {
        NonTransactionalCdc,
        FullLoad
    }
}
