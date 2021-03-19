using System;
using System.Collections.Generic;
using System.Text;
using RabbitMQ.Client;

namespace Summer.Batch.Data
{
    public class ControlQueue : IQueue
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

        /// <summary>
        /// Create messageQueue
        /// </summary>
        public void CreateQueue()
        {
            if (string.IsNullOrEmpty(QueueName))
            {
                throw new ArgumentNullException("QueueName");
            }
            else
            {
                Channel.QueueDeclare(QueueName, Durable, Exclusive, AutoDelete, Arguments);
                Channel.BasicQos(0, 1, false);
            }
        }

        //public void CloseQueue(string queueName)
        //{
        //    Channel.QueueDelete(queueName);
        //    ConnectionProvider.Connection.Close();
        //}

        /// <summary>
        /// Push the message in the queue
        /// </summary>
        /// <param name="message"></param>
        public void Send(string message)
        {
            if (string.IsNullOrWhiteSpace(message))
                return;

            var body = Encoding.UTF8.GetBytes(message);
            Channel.BasicPublish(exchange: "",
                               routingKey: QueueName,
                               basicProperties: null,
                               body: body);
        }

        /// <summary>
        /// Receive the message from the queue
        /// </summary>
        /// <param name="message"></param>
        /// <returns></returns>
        public string Receive(string message)
        {
            BasicGetResult result = Channel.BasicGet(QueueName, false);
            string data = null;
            if (result != null)
            {
                data = Encoding.UTF8.GetString(result.Body.ToArray());
                if (data.Contains(message))
                {
                    Channel.BasicAck(result.DeliveryTag, false);
                    return data;
                }
            }
            return null;
        }

        /// <summary>
        /// Receive total numbers of message in the queue
        /// </summary>
        /// <returns></returns>
        public int GetMessageCount()
        {
            return Convert.ToInt32(Channel.MessageCount(QueueName));
        }

        /// <summary>
        /// Recovery message status to the ready
        /// </summary>
        public void Requeue()
        {
            Channel.BasicRecover(true);
        }

        /// <summary>
        /// Purge message in the queue
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
        /// Count the targetmessage in the queue
        /// </summary>
        /// <param name="controlQueue"></param>
        /// <param name="targetmessage"></param>
        /// <returns></returns>
        public int CheckMessageCount(string targetmessage)
        {
            Requeue();
            int TotalCount = GetMessageCount();
            int count = 0;
            while (TotalCount > 0)
            {
                string message = Receive(targetmessage);
                if (message != null)
                {
                    count++;
                }
                TotalCount--;
            }
            Requeue();
            return count;
        }
    }
}
