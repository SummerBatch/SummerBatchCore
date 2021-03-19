using System;
using System.Collections.Generic;
using System.Text;

namespace Summer.Batch.Data
{
    public interface IQueue
    {
        string QueueName { get; set; }
        IQueueConnectionProvider ConnectionProvider { get; set; }
    }
}
