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
        public int Port { get; set; }
    }
}
