using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;

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

        private const string SlaveLifeLineQueue = "slave_lifeline";

        private const string MasterLifeLienQueue = "master_lifeline";

        public bool _master;

        public Dictionary<string, bool> _slaveMap;

        public TimeSpan _maxTimeOut;

        [NonSerialized]
        private AutoResetEvent _threadWait;
        [NonSerialized]
        private Thread _thread;
        [NonSerialized]
        public ControlQueue _controlQueue;
        [NonSerialized]
        public ControlQueue _masterQueue;
        [NonSerialized]
        public ControlQueue _slaveCompletedQueue;
        [NonSerialized]
        public ControlQueue _slaveStartedQueue;
        [NonSerialized]
        public ControlQueue _slaveLifeLineQueue;
        [NonSerialized]
        public ControlQueue _masterLifeLineQueue;

        public AutoResetEvent threadWait 
        { 
            set { _threadWait = value; } 
            get { return _threadWait; } 
        }

        public string SlaveID { set; get; }


        public Thread controlThread
        {
            set { _thread = value; }
            get { return _thread; }
        }

        public RemoteChunking(string hostname, bool master)
        {
            _hostname = hostname;
            _master = master;
            _controlQueue = CreateQueue(ControlQueue);
            _masterQueue = CreateQueue(MasterQueue);
            _slaveCompletedQueue = CreateQueue(SlaveCompletedQueue);
            _slaveStartedQueue = CreateQueue(SlaveStartedQueue);
            _slaveLifeLineQueue = CreateQueue(SlaveLifeLineQueue);
            _masterLifeLineQueue = CreateQueue(MasterLifeLienQueue);
            if (_master)
            {
                _slaveMap = new Dictionary<string, bool>();
                _maxTimeOut = new TimeSpan(6000);
            }
        }

        public ControlQueue CreateQueue(string queueName)
        {
            if (String.IsNullOrWhiteSpace(_hostname))
            {
                throw new ArgumentException("HostName was not found. You must specify hostname to start remotechunking in a step.");

            }

            QueueConnectionProvider queueConnectionProvider = new QueueConnectionProvider();
            queueConnectionProvider.HostName = _hostname;
            ControlQueue controlQueue = new ControlQueue();
            controlQueue.ConnectionProvider = queueConnectionProvider;
            controlQueue.QueueName = queueName;
            controlQueue.CreateQueue();

            return controlQueue;
        }

        public void CleanAllQueue()
        {
            _controlQueue.PurgeQueue();
            _masterQueue.PurgeQueue();
            _slaveCompletedQueue.PurgeQueue();
            _slaveStartedQueue.PurgeQueue();
            _slaveLifeLineQueue.PurgeQueue();
            _masterLifeLineQueue.PurgeQueue();
        }
    }
}
