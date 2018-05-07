using CdcTools.CdcReader.Changes;
using CdcTools.CdcReader.Tables;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace CdcTools.CdcReader.State
{
    public interface IStateManager
    {
        Task StoreCdcOffsetAsync(string executionId, string tableName, Offset offset);
        Task<StateResult<Offset>> GetLastCdcOffsetAsync(string executionId, string tableName);
        Task StorePkOffsetAsync(string executionId, string tableName, PrimaryKeyValue pkValue);
        Task<StateResult<PrimaryKeyValue>> GetLastPkOffsetAsync(string executionId, string tableName);
    }
}
