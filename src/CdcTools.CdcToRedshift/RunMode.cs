using System;
using System.Collections.Generic;
using System.Text;

namespace CdcTools.CdcToRedshift
{
    public enum RunMode
    {
        NonTransactionalCdc,
        TransactionalCdc,
        FullLoad
    }
}
