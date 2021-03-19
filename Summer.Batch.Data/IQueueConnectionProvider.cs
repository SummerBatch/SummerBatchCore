using System;
using System.Collections.Generic;
using System.Text;
using RabbitMQ.Client;

namespace Summer.Batch.Data
{
    public interface IQueueConnectionProvider
    {
        string HostName { get; set; }
        ConnectionFactory ConnectionFactory { get; }

        IConnection Connection { get; }

        IModel Channel { get; }
    }
}
