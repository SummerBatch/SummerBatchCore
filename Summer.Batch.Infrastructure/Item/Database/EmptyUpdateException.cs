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
using System;

namespace Summer.Batch.Infrastructure.Item.Database
{
    /// <summary>
    /// Exception thrown when an insert or an update query is expected to affect at least one record but no records were affected.
    /// </summary>
    [Serializable]
    public class EmptyUpdateException : Exception
    {
        /// <summary>
        /// Constructs a new <see cref="EmptyUpdateException"/> with the specified message
        /// </summary>
        /// <param name="message">the detail message</param>
        public EmptyUpdateException(string message) : base(message)
        {
        }
    }
}