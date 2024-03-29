﻿using System;
using System.Collections.Generic;
using System.Text;
using RabbitMQ.Client;

namespace Summer.Batch.Data
{
    public class DataQueue
    {
        public string HostName { get; set; } = "localhost";

        public string UserName { get; set; } = "admin";

        public string PassWord { get; set; } = "admin";

        public string QueueName { get; set; }

        public bool Durable { get; set; }

        public bool Exclusive { get; set; }

        public bool AutoDelete { get; set; }

        public IDictionary<string, object> Arguments { get; set; }

        public IModel Channel { get; private set; }

        /// <summary>
        /// Create messageQueue with HostName and QueueName.
        /// </summary>
        public void CreateQueue()
        {
            if (string.IsNullOrEmpty(QueueName) || string.IsNullOrEmpty(HostName) || string.IsNullOrEmpty(UserName) || string.IsNullOrEmpty(PassWord))
            {
                throw new ArgumentNullException("QueueName, HostName, UserName, and PassWord need to provide.");
            }
            else
            {
                ConnectionFactory connectionFactory = new ConnectionFactory() { HostName = HostName, Password = UserName, UserName = PassWord };
                IConnection connection = connectionFactory.CreateConnection();
                Channel = connection.CreateModel();
                Channel.QueueDeclare(QueueName, Durable, Exclusive, AutoDelete, Arguments);
                Channel.BasicQos(0, 1, false);
            }
        }
    }
}
