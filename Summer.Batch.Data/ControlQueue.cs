using System;
using System.Collections.Generic;
using System.Text;
using NLog;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Summer.Batch.Data
{
    public class ControlQueue
    {
        private const string dot = ".";
        private readonly Logger _logger = LogManager.GetCurrentClassLogger();
        public string QueueName { get; set; }

        public bool Durable { get; set; }

        public bool Exclusive { get; set; }

        public bool AutoDelete { get; set; }

        public IDictionary<string, object> Arguments { get; set; }

        /// <summary>
        /// Inject ConnectionProvider
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
        /// Receive the message from the queue
        /// </summary>
        /// <param name="message"></param>
        /// <returns></returns>
        public string Consume()
        {
            BasicGetResult result = Channel.BasicGet(QueueName, true);
            if (result != null)
            {
                string data = Encoding.UTF8.GetString(result.Body.ToArray());
                return data;

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
        /// Check message in the message queue and consume it all.
        /// </summary>
        /// <param name="targetmessage"></param>
        /// <returns></returns>
        public void CheckMessageExistAndConsumeAll(List<string> workerIDs, Dictionary<string, bool> workerMap)
        {
            string TrueMessage = bool.TrueString;
            int TotalCount = GetMessageCount();

            foreach(string ID in workerIDs)
            {
                workerMap[ID] = false;
            }
            while (TotalCount > 0)
            {
                string message = Consume();
                if (ValidateMessage(message))
                {
                    string[] splitMessage = message.Split(dot);
                    string ID = splitMessage[0] + dot + splitMessage[1];
                    bool IsAlive = (bool.TryParse(splitMessage[2], out bool value)) ? value : false;

                    if (workerIDs.Contains(ID))
                    {
                        if (IsAlive)
                        {
                            workerMap[ID] = true;
                        }
                        else
                        {
                            workerMap[ID] = false;
                        }
                    }
                    else
                    {
                        workerMap[ID] = false;
                    }
                }
                TotalCount--;
            }

            foreach (string ID in workerIDs)
            {
                if (workerMap[ID])
                {
                    _logger.Debug("workerkey: " + ID + " -----------Alive------------------");
                }
                else
                {
                    _logger.Debug("workerkey: " + ID + " -----------NoAlive------------------");
                }

            }

        }

        private static bool ValidateMessage(string message)
        {
            if (!string.IsNullOrWhiteSpace(message) && message.Split(dot).Length == 4)
            {
                return true;
            }
            return false;
        }

        /// <summary>
        /// Retrieve list of workerID with master name.
        /// </summary>
        /// <param name="master"></param>
        /// <returns></returns>
        public List<string> GetWorkerIDByMasterName(string master)
        {
            Requeue();
            int messageCount = GetMessageCount();
            List<string> workerIDList = new List<string>();
            while (messageCount > 0)
            {
                string workerID = Receive(master);
                if (workerID != null)
                {
                    workerIDList.Add(workerID);
                }
                messageCount--;
            }
            Requeue();
            return workerIDList;
        }
    }
}
