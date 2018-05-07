using System;
using System.Collections.Generic;
using System.Text;

namespace CdcTools.CdcReader.State
{
    public enum Result
    {
        NoStoredState,
        StateReturned
    }

    public class StateResult<T>
    {
        public StateResult(Result result, T state)
        {
            Result = result;
            State = state;
        }

        public Result Result { get; set; }
        public T State { get; set; }
    }
}
