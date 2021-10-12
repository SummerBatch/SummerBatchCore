using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace Summer.Batch.Data
{
    [Serializable]
    public class RemoteChunking
    {
        /// <summary>
        /// server of message queue
        /// </summary>
        private string _hostname, _username, _password;

        private const string ControlQueue = "control";

        private const string MasterQueue = "master";

        private const string WorkerCompletedQueue = "worker_completed";

        private const string WorkerStartedQueue = "worker_started";

        private const string WorkerLifeLineQueue = "worker_lifeline";

        private const string MasterLifeLienQueue = "master_lifeline";

        /// <summary>
        /// master flag
        /// </summary>
        public bool _master;

        /// <summary>
        /// hashmap for master to store worker id.
        /// </summary>
        public Dictionary<string, bool> _workerMap;

        
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
        public ControlQueue _workerCompletedQueue;
        [NonSerialized]
        public ControlQueue _workerStartedQueue;
        [NonSerialized]
        public ControlQueue _workerLifeLineQueue;
        [NonSerialized]
        public ControlQueue _masterLifeLineQueue;

        /// <summary>
        /// event to control second thread
        /// </summary>
        public AutoResetEvent threadWait 
        { 
            set { _threadWait = value; } 
            get { return _threadWait; } 
        }

        /// <summary>
        ///  unique worker id
        /// </summary>
        public string WorkerID { set; get; }

        /// <summary>
        /// unique worker file name
        /// </summary>
        public string WorkerFileName { set; get; }

        /// <summary>
        /// max number of worker
        /// </summary>
        public int WorkerMaxNumber { set; get; }

        /// <summary>
        /// max retry for master
        /// </summary>
        public int MaxMasterWaitWorkerRetry { set; get; }

        /// <summary>
        /// max time for master every retry
        /// </summary>
        public int MaxMasterWaitWorkerSecond { set; get; }

        /// <summary>
        /// timeout of remotechunking
        /// </summary>
        public TimeSpan RemoteChunkingTimoutSecond { set; get; }
        /// <summary>
        /// control thread to access message queue
        /// </summary>
        public Thread controlThread
        {
            set { _thread = value; }
            get { return _thread; }
        }

        public RemoteChunking(string hostname, string username, string password, bool master)
        {
            _hostname = hostname;
            _username = username;
            _password = password;
            _master = master;
            _controlQueue = CreateQueue(ControlQueue);
            _masterQueue = CreateQueue(MasterQueue);
            _workerCompletedQueue = CreateQueue(WorkerCompletedQueue);
            _workerStartedQueue = CreateQueue(WorkerStartedQueue);
            _workerLifeLineQueue = CreateQueue(WorkerLifeLineQueue);
            _masterLifeLineQueue = CreateQueue(MasterLifeLienQueue);

            //master need to initialize hashmap 
            if (_master)
            {
                _workerMap = new Dictionary<string, bool>();
                _maxTimeOut = new TimeSpan(6000);
            }
        }

        /// <summary>
        /// Create message queue with queueName.
        /// </summary>
        /// <param name="queueName"></param>
        /// <returns></returns>
        public ControlQueue CreateQueue(string queueName)
        {
            if (String.IsNullOrWhiteSpace(_hostname))
            {
                throw new ArgumentException("HostName was not found. You must specify hostname to start remotechunking in a step.");

            }

            QueueConnectionProvider queueConnectionProvider = new QueueConnectionProvider();
            queueConnectionProvider.UserName = _username;
            queueConnectionProvider.PassWord = _password;
            queueConnectionProvider.HostName = _hostname;
            ControlQueue controlQueue = new ControlQueue();
            controlQueue.ConnectionProvider = queueConnectionProvider;
            controlQueue.QueueName = queueName;
            controlQueue.CreateQueue();

            return controlQueue;
        }

        /// <summary>
        /// Clean content of all message queue.
        /// </summary>
        public void CleanAllQueue()
        {
            _controlQueue.PurgeQueue();
            _masterQueue.PurgeQueue();
            _workerCompletedQueue.PurgeQueue();
            _workerStartedQueue.PurgeQueue();
            _workerLifeLineQueue.PurgeQueue();
            _masterLifeLineQueue.PurgeQueue();
        }
    }
}
