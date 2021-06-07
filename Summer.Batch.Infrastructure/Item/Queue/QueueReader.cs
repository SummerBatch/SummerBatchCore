using System;
using System.Collections.Generic;
using System.Text;
using System.Text.Json;
using System.Threading;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using NLog;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Summer.Batch.Common.Factory;
using Summer.Batch.Common.Util;
using Summer.Batch.Data;

namespace Summer.Batch.Infrastructure.Item.Queue
{
    public class QueueReader<T> : IItemReader<T>, IInitializationPostOperations where T : class
    {
        private static readonly Logger _logger = LogManager.GetCurrentClassLogger();
        private DataQueue _dataQueue;
        private const string dot = ".";

        /// <summary>
        /// Timeout for polling data
        /// </summary>
        public int PollingTimeOut { get; set; } = 1;

        /// <summary>
        /// Maximum Number of polling
        /// </summary>
        public int MaxNumberOfPolls { get; set; } = 15;


        /// <summary>
        /// Master step for worker to connect
        /// </summary>
        public string MasterName { get; set; }

        /// <summary>
        /// Data queue to store data
        /// </summary>
        public DataQueue DataQueue
        {
            set
            {
                _dataQueue = value;
                _dataQueue.CreateQueue();
            }
        }
        public void AfterPropertiesSet()
        {
            Assert.NotNull(_dataQueue, "DataQueue must be provided");
            Assert.NotNull(MasterName, "MasterName must be provided");
        }


        public T Read()
        {
            T item = null;
            string data = null;
            bool firstTimeout = true;

            while (MaxNumberOfPolls > 0)
            {
                // Read Message from the dataQueue
                BasicGetResult result = _dataQueue.Channel.BasicGet(_dataQueue.QueueName, true);
                if (result != null)
                {
                    try
                    {
                        // Deserialize message to business object
                        item = SerializationUtils.DeserializeObject<T>(result.Body.ToArray());
                        break;
                    }
                    catch (Exception ex)
                    {
                        _logger.Debug("Json Serialize failed. Reason: " + ex.StackTrace);
                        throw;
                    }
                }
                else
                {
                    // If there is no data to poll, wait for master step
                    _logger.Debug("Wait for {0} second for dataQueue, check for master part is completed or not", PollingTimeOut);
                    Thread.Sleep(TimeSpan.FromSeconds(PollingTimeOut));

                    // For the first timeout , check master step is completed
                    if (!string.IsNullOrWhiteSpace(MasterName))
                    {
                        ControlQueue _masterqueue = new ControlQueue();
                        QueueConnectionProvider queueConnectionProvider = new QueueConnectionProvider();
                        queueConnectionProvider.HostName = _dataQueue.HostName;
                        _masterqueue.ConnectionProvider = queueConnectionProvider;
                        _masterqueue.QueueName = "master";
                        _masterqueue.CreateQueue();

                        string MasterCompletedMessage = "master" + dot + MasterName + dot + "COMPLETED";

                        if (firstTimeout)
                        {
                            _logger.Debug("First Timeout need to check the master process is completed. ");
                            firstTimeout = false;

                            if (_masterqueue.CheckMessageExistAndConsume(MasterCompletedMessage))
                            {
                                _logger.Debug("There is no more data to read for worker.");
                                break;
                            }
                            _logger.Debug("Need to wait for master to completed.");
                        }
                    }
                    MaxNumberOfPolls--;
                }
            }
           

            return item;
        }
    }
}
