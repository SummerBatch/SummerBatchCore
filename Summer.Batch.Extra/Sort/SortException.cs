﻿//   Copyright 2015 Blu Age Corporation - Plano, Texas
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.

using System;
using System.Runtime.Serialization;

namespace Summer.Batch.Extra.Sort
{
    /// <summary>
    /// Exception thrown when there are problems in a sort step. More specific sort exceptions should inherit from it.
    /// </summary>
    public class SortException : Exception
    {
        /// <summary>
        /// Constructs a new <see cref="SortException"/> with a message.
        /// </summary>
        /// <param name="message">the error message</param>
        public SortException(string message) : base(message) { }

        /// <summary>
        /// Constructor for deserialization.
        /// </summary>
        /// <param name="info">the info holding the serialization data</param>
        /// <param name="context">the serialization context</param>
        protected SortException(SerializationInfo info, StreamingContext context) : base(info, context) { }
    }
}