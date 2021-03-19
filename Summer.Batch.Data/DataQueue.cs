using System;
using System.Collections.Generic;
using System.Text;
using RabbitMQ.Client;

namespace Summer.Batch.Data
{
    public class DataQueue : IQueue
    {
        public string QueueName { get; set; }

        public bool Durable { get; set; }

        public bool Exclusive { get; set; }

        public bool AutoDelete { get; set; }

        public IDictionary<string, object> Arguments { get; set; }

        public IQueueConnectionProvider ConnectionProvider { get; set; }


        public IModel Channel
        {
            get { return ConnectionProvider.Channel; }
        }

        public void CreateQueue()
        {
            if (string.IsNullOrEmpty(QueueName))
            {
                throw new ArgumentNullException("QueueName");
            }
            else
            {
                Channel.QueueDeclare(QueueName, Durable, Exclusive, AutoDelete, Arguments);
                Channel.BasicQos(0, 1000, false);
            }
        }
    }
}
