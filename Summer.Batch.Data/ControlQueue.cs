using System;
using System.Collections.Generic;
using System.Text;
using NLog;
using RabbitMQ.Client;

namespace Summer.Batch.Data
{
    public class ControlQueue
    {
        private readonly Logger _logger = LogManager.GetCurrentClassLogger();
        public string QueueName { get; set; }

        public bool Durable { get; set; }

        public bool Exclusive { get; set; }

        public bool AutoDelete { get; set; }

        public IDictionary<string, object> Arguments { get; set; }
        
        /// <summary>
        /// Inject Conn
        /// </summary>
        public QueueConnectionProvider ConnectionProvider { get; set; }


        public IModel Channel
        {
            get { return ConnectionProvider.Channel; }
        }

        /// <summary>
        /// Create messageQueue with QueueName.
        /// </summary>
        public void CreateQueue()
        {
            if (string.IsNullOrEmpty(QueueName) || ConnectionProvider == null)
            {
                throw new ArgumentNullException("QueueName and ConnectionProvider need to provide.");
            }
            else
            {
                Channel.QueueDeclare(QueueName, Durable, Exclusive, AutoDelete, Arguments);
                Channel.BasicQos(0, 1, false);
            }
        }
        /// <summary>
        /// Push the message in the queue
        /// </summary>
        /// <param name="message"></param>
        public void Send(string message)
        {
            if (string.IsNullOrWhiteSpace(message))
                return;

            byte[] bytes = Encoding.UTF8.GetBytes(message);
            IBasicProperties basicProperties = Channel.CreateBasicProperties();
            basicProperties.ContentType = "text/plain";
            Channel.BasicPublish(exchange: "",
                               routingKey: QueueName,
                               basicProperties: null,
                               body: bytes);
        }

        /// <summary>
        /// Receive the message from the queue
        /// </summary>
        /// <param name="message"></param>
        /// <returns></returns>
        public string Receive(string message)
        {
            BasicGetResult result = Channel.BasicGet(QueueName, false);
            if (result != null)
            {
                string data = Encoding.UTF8.GetString(result.Body.ToArray());
                if (data.Contains(message))
                {
                    Channel.BasicAck(result.DeliveryTag, false);
                    return data;
                }
            }
            return null;
        }

        /// <summary>
        /// Receive total numbers of message in the queue.
        /// </summary>
        /// <returns></returns>
        public int GetMessageCount()
        {
            return Convert.ToInt32(Channel.MessageCount(QueueName));
        }

        /// <summary>
        /// Recovery message status to the ready.
        /// </summary>
        public void Requeue()
        {
            Channel.BasicRecover(true);
        }

        /// <summary>
        /// Purge message in the queue.
        /// </summary>
        public void PurgeQueue()
        {
            try
            {
                if (GetMessageCount() > 0)
                    Channel.QueuePurge(QueueName);
            }
            catch (Exception e)
            {

                Console.WriteLine("Exception {0} occured.", e.ToString());
            }
        }

        /// <summary>
        /// Check message in the message queue.
        /// </summary>
        /// <param name="targetmessage"></param>
        /// <returns></returns>
        public bool CheckMessageExist(string targetmessage)
        {
            Requeue();
            int TotalCount = GetMessageCount();
            while (TotalCount > 0)
            {
                string message = Receive(targetmessage);
                if (message != null)
                {
                    Send(message);
                    return true;
                }
                TotalCount--;
            }
            Requeue();
            return false;
        }

        /// <summary>
        /// Check message in the message queue and consume it.
        /// </summary>
        /// <param name="targetmessage"></param>
        /// <returns></returns>
        public bool CheckMessageExistAndConsume(string targetmessage)
        {
            Requeue();
            int TotalCount = GetMessageCount();
            while (TotalCount > 0)
            {
                string message = Receive(targetmessage);
                if (message != null)
                {
                    return true;
                }
                TotalCount--;
            }
            Requeue();
            return false;
        }

        /// <summary>
        /// Retrieve list of slaveID with master name.
        /// </summary>
        /// <param name="master"></param>
        /// <returns></returns>
        public List<string> GetSlaveIDByMasterName(string master)
        {
            Requeue();
            int messageCount = GetMessageCount();
            List<string> slaveIDList = new List<string>();
            while (messageCount > 0)
            {
                string slaveID = Receive(master);
                if (slaveID != null)
                {
                    slaveIDList.Add(slaveID);
                }
                messageCount--;
            }
            Requeue();
            return slaveIDList;
        }
    }
}
