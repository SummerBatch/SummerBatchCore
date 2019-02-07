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
namespace Summer.Batch.Extra.Sort.Legacy.Accessor
{
    /// <summary>
    /// Implementation of <see cref="IAccessor{T}"/> for string values.
    /// </summary>
    public class StringAccessor : AbstractAccessor<string>
    {
        /// <summary>
        /// Gets a value from a record.
        /// </summary>
        /// <param name="record">the record to get the value from</param>
        /// <returns>the read value</returns>
        public override string Get(byte[] record)
        {
            return Encoding.GetString(record, Start, Length);
        }

        /// <summary>
        /// Sets a value on a record.
        /// </summary>
        /// <param name="record">the record to set the value on</param>
        /// <param name="value">the value to set</param>
        public override void Set(byte[] record, string value)
        {
            SetBytes(record, Encoding.GetBytes(value), Encoding.GetBytes(" ")[0]);
        }
    }
}