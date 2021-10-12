using System;
using System.Collections.Generic;
using System.Text;
using System.Text.Json;
using Newtonsoft.Json;
using NLog;
using RabbitMQ.Client;
using Summer.Batch.Common.Factory;
using Summer.Batch.Common.Util;
using Summer.Batch.Data;

namespace Summer.Batch.Infrastructure.Item.Queue
{
    public class QueueWriter<T> : IItemWriter<T>, IInitializationPostOperations where T : class
    {
        private readonly Logger _logger = LogManager.GetCurrentClassLogger();

        private DataQueue _dataQueue;
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

        public void Write(IList<T> items)
        {
            _logger.Debug("Executing batch queue writer with {0} items.", items.Count);

            if (items.Count == 0)
            {
                _logger.Warn("Executing batch queue writer : empty list of items has been given.");
                return;
            }
            foreach (var item in items)
            {
                try
                {
                    // serilize business object to byte array.
                    byte[] data = SerializationUtils.SerializeObject(item);
                    IBasicProperties properties = _dataQueue.Channel.CreateBasicProperties();
                    properties.ContentType = "application/json";
                    _dataQueue.Channel.BasicPublish(exchange: "",
                                         routingKey: _dataQueue.QueueName,
                                         basicProperties: properties,
                                         body: data);
                }
                catch (Exception e)
                {
                    _logger.Debug("Json Serialize failed. Reason: " + e.InnerException);
                    throw;
                }

            }

        }

    }
}