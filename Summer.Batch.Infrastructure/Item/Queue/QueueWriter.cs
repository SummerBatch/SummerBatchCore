using System;
using System.Collections.Generic;
using System.Text;
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
                String jsonItems = JsonConvert.SerializeObject(item);
                var body = Encoding.UTF8.GetBytes(jsonItems);
                _dataQueue.Channel.BasicPublish(exchange: "",
                                     routingKey: _dataQueue.QueueName,
                                     basicProperties: null,
                                     body: body);
            }
        }
    }
}
