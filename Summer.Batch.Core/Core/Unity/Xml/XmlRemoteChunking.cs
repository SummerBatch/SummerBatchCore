using System;
using System.Collections.Generic;
using System.Text;
using System.Xml.Serialization;

namespace Summer.Batch.Core.Core.Unity.Xml
{
    public class XmlRemoteChunking
    {
        /// <summary>
        /// hostname attribute.
        /// </summary>
        [XmlAttribute("hostname")]
        public string HostName { get; set; } = "localhost";

        /// <summary>
        /// username attribute.
        /// </summary>
        [XmlAttribute("username")]
        public string UserName { get; set; } = "admin";

        /// <summary>
        /// password attribute.
        /// </summary>
        [XmlAttribute("password")]
        public string PassWord { get; set; } = "admin";

        /// <summary>
        /// master attribute.
        /// </summary>
        [XmlAttribute("master")]
        public bool Master { get; set; } = true;

        /// <summary>
        /// port attribute.
        /// </summary>
        [XmlAttribute("port")]
        public string Port { get; set; }

        /// <summary>
        /// unique worker id.
        /// </summary>
        [XmlAttribute("workerID")]
        public string WorkerID { get; set; }

        /// <summary>
        /// unique worker file name.
        /// </summary>
        [XmlAttribute("workerFileName")]
        public string WorkerFileName { get; set; } = "Worker.xml";

        /// <summary>
        /// max number of worker
        /// </summary>
        [XmlAttribute("workerMaxNumber")]
        public string WorkerMaxNumber { get; set; } = "2";


        /// <summary>
        /// max retry for master
        /// </summary>
        [XmlAttribute("maxMasterWaitWorkerRetry")]
        public string MaxMasterWaitWorkerRetry { get; set; } = "3";

        /// <summary>
        /// max time for master every retry
        /// </summary>
        [XmlAttribute("maxMasterWaitWorkerSecond")]
        public string MaxMasterWaitWorkerSecond { get; set; } = "5";

        /// <summary>
        /// timeout of remotechunking
        /// </summary>
        [XmlAttribute("remoteChunkingTimoutSecond")]
        public string RemoteChunkingTimoutSecond { get; set; } = "900";
    }
}
