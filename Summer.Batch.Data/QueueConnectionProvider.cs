using System;
using System.Collections.Generic;
using System.Text;
using RabbitMQ.Client;

namespace Summer.Batch.Data
{
    public class QueueConnectionProvider
    {
        private string _hostname;

        public string HostName
        {
            set
            {
                _hostname = value;
                ConnectionFactory = new ConnectionFactory { HostName = _hostname, Password = "admin", UserName = "admin" };
                Connection = ConnectionFactory.CreateConnection();
                Channel = Connection.CreateModel();

            }
            get
            {
                return this._hostname;
            }
        }
        public ConnectionFactory ConnectionFactory { get; private set; }

        public IConnection Connection { get; private set; }

        public IModel Channel { get; private set; }

    }
}
