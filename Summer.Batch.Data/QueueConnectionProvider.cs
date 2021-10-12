using System;
using System.Collections.Generic;
using System.Text;
using RabbitMQ.Client;

namespace Summer.Batch.Data
{
    public class QueueConnectionProvider
    {
        
        private string _hostname, _username, _password;

        /// <summary>
        /// username attribute for connection
        /// </summary>
        public string UserName 
        {
            get
            {
                return _username;
            }

            set
            {
                this._username = value;
            }
        }

        /// <summary>
        /// password attribute for connection
        /// </summary>
        public string PassWord 
        {
            get
            {
                return _password;
            }

            set
            {
                this._password = value;
            }
        }
        
        /// <summary>
        /// server of connection
        /// </summary>
        public string HostName
        {
            set
            {
                _hostname = value;

                if (!string.IsNullOrWhiteSpace(_username) && !string.IsNullOrWhiteSpace(_password) && !string.IsNullOrWhiteSpace(_hostname))
                {
                    ConnectionFactory = new ConnectionFactory { HostName = _hostname, Password = _username, UserName = _password };
                    Connection = ConnectionFactory.CreateConnection();
                    Channel = Connection.CreateModel();
                }
                else
                {
                    throw new ArgumentNullException("UserName, PassWord ,and HostName need to provide.");
                }
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
