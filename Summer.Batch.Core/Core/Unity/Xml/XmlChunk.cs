﻿//
//   Copyright 2015 Blu Age Corporation - Plano, Texas
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
using System.Xml.Serialization;

namespace Summer.Batch.Core.Unity.Xml
{
    /// <summary>
    /// Xml representation of chunk.
    /// </summary>
    public class XmlChunk
    {
        /// <summary>
        /// reader attribute.
        /// </summary>
        [XmlElement("reader")]
        public XmlItemReader Reader { get; set; }
        
        /// <summary>
        /// processor attribute.
        /// </summary>
        [XmlElement("processor")]
        public XmlItemProcessor Processor { get; set; }
        
        /// <summary>
        /// writer attribute.
        /// </summary>
        [XmlElement("writer")]
        public XmlItemWriter Writer { get; set; }

        /// <summary>
        /// item-count attribute.
        /// </summary>
        [XmlAttribute("item-count")]
        public string ItemCount { get; set; }
    }
}