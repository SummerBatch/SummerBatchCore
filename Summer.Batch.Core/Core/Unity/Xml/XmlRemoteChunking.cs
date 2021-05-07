﻿using System;
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
        public string HostName { get; set; }

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
        /// unique slave id.
        /// </summary>
        [XmlAttribute("slaveID")]
        public string SlaveID { get; set; }

        /// <summary>
        /// unique slave file name.
        /// </summary>
        [XmlAttribute("slaveFileName")]
        public string SlaveFileName { get; set; } = "Slave.xml";

        /// <summary>
        /// max number of slave
        /// </summary>
        [XmlAttribute("slaveMaxNumber")]
        public string SlaveMaxNumber { get; set; } = "2";


        /// <summary>
        /// max retry for master
        /// </summary>
        [XmlAttribute("maxMasterWaitSlaveRetry")]
        public string MaxMasterWaitSlaveRetry { get; set; } = "3";

        /// <summary>
        /// max time for master every retry
        /// </summary>
        [XmlAttribute("maxMasterWaitSlaveSecond")]
        public string MaxMasterWaitSlaveSecond { get; set; } = "5";

        /// <summary>
        /// timeout of remotechunking
        /// </summary>
        [XmlAttribute("remoteChunkingTimoutSecond")]
        public string RemoteChunkingTimoutSecond { get; set; } = "900";
    }
}
