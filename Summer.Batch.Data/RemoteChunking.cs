using System;
using System.Collections.Generic;
using System.Text;

namespace Summer.Batch.Data
{
    [Serializable]
    public class RemoteChunking
    {
        private string _hostname;

        private const string ControlQueue = "control";

        private const string MasterQueue = "master";

        private const string SlaveCompletedQueue = "slave_completed";

        private const string SlaveStartedQueue = "slave_started";

        public bool _master;


        [NonSerialized]
        public ControlQueue _controlQueue;
        [NonSerialized]
        public ControlQueue _masterQueue;
        [NonSerialized]
        public ControlQueue _slaveCompletedQueue;
        [NonSerialized]
        public ControlQueue _slaveStartedQueue;

        public RemoteChunking(string hostname, bool master)
        {
            _hostname = hostname;
            _master = master;
            _controlQueue = CreateQueue(ControlQueue);
            _masterQueue = CreateQueue(MasterQueue);
            _slaveCompletedQueue = CreateQueue(SlaveCompletedQueue);
            _slaveStartedQueue = CreateQueue(SlaveStartedQueue);
        }

        public ControlQueue CreateQueue(string queueName)
        {
            if (String.IsNullOrWhiteSpace(_hostname))
            {
                throw new ArgumentException("HostName was not found. You must specify hostname to start remotechunking in a step.");

            }

            QueueConnectionProvider queueConnectionProvider = new QueueConnectionProvider();
            queueConnectionProvider.HostName = _hostname;
            ControlQueue _queue = new ControlQueue();
            _queue.ConnectionProvider = queueConnectionProvider;
            _queue.QueueName = queueName;
            _queue.CreateQueue();

            return _queue;
        }

        public void CleanAllQueue()
        {
            _controlQueue.PurgeQueue();
            _masterQueue.PurgeQueue();
            _slaveCompletedQueue.PurgeQueue();
            _slaveStartedQueue.PurgeQueue();
        }
    }
}
