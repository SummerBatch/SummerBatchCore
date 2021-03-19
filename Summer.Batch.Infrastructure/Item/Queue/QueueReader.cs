using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using Newtonsoft.Json;
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
        private static readonly Logger Logger = LogManager.GetCurrentClassLogger();
        private DataQueue _dataQueue;

        /// <summary>
        /// Timeout for polling data
        /// </summary>
        public int PollingTimeOut { get; set; } = 2000;

        /// <summary>
        /// Maximum Number of polling
        /// </summary>
        public int MaxNumberOfPulls { get; set; } = 5;

        /// <summary>
        /// Master step for slave to connect
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
        }


        public T Read()
        {
            var consumer = new EventingBasicConsumer(_dataQueue.Channel);
            string data = null;
            bool firstTimeout = true;

            for (int i = 0; i < MaxNumberOfPulls; i++)
            {
                // Read Message from the dataQueue
                BasicGetResult result = _dataQueue.Channel.BasicGet(_dataQueue.QueueName, true);
                if (result != null)
                {
                    data = Encoding.UTF8.GetString(result.Body.ToArray());
                    break;
                }
                else
                {
                    // If there is no data to poll, wait for master step
                    Thread.Sleep(PollingTimeOut);
                    Logger.Debug("Wait for {0} second for dataQueue, check for master part is completed or not", TimeSpan.FromMilliseconds(PollingTimeOut).TotalSeconds.ToString());

                    // For the first timeout , check master step is completed
                    if (!string.IsNullOrWhiteSpace(MasterName) && firstTimeout)
                    {

                        Logger.Debug("First Timeout need to check the master process is completed. ");
                        firstTimeout = false;
                        ControlQueue _masterqueue = new ControlQueue();
                        _masterqueue.ConnectionProvider = _dataQueue.ConnectionProvider;
                        _masterqueue.QueueName = "master";
                        _masterqueue.CreateQueue();
                        MasterName = "master." + MasterName + ".COMPLETED";

                        if (_masterqueue.CheckMessageCount(MasterName) > 0)
                        {
                            Logger.Debug("There is no more data to read, slave");
                            _masterqueue.Send(MasterName);
                            break;
                        }
                        Logger.Debug("Need to wait for master to completed.");
                    }

                }
            }

            return (data != null) ? JsonConvert.DeserializeObject<T>(data) : null;
        }
    }
}
