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
using System.Collections.Generic;
using Summer.Batch.Infrastructure.Item;

namespace Summer.Batch.CoreTests.Delegating
{
    public class VolatileWriter : IItemWriter<CommandItem>
    {
        public List<CommandItem> Out { get; set; } 
        public void Write(IList<CommandItem> items)
        {
            Out = new List<CommandItem>(items.Count);
            Out.AddRange(items);
        }
    }
}