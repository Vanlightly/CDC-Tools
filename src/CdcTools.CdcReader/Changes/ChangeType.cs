using System;
using System.Collections.Generic;
using System.Text;

namespace CdcTools.CdcReader.Changes
{
    public enum ChangeType
    {
        NOT_DEFINED = 0,
        DELETE = 1,
        INSERT = 2,
        UPDATE_BEFORE = 3,
        UPDATE_AFTER = 4
    }
}
