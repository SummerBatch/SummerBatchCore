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
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.

//   This file has been modified.
//   Original copyright notice :

/*
 * Copyright 2006-2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System.Collections.Generic;
using System.Linq;

namespace Summer.Batch.Core.Job.Flow.Support.State
{
    /// <summary>
    /// Implementation of the <see cref="IFlowExecutionAggregator"/> interface that aggregates
    /// FlowExecutionStatus, using the status with the high precedence as the
    /// aggregate status.  See <see cref="FlowExecutionStatus"/> for details on status
    /// precedence.
    /// </summary>
    public class MaxValueFlowExecutionAggregator : IFlowExecutionAggregator
    {
        /// <summary>
        /// Aggregates all of the FlowExecutionStatuses of the
        /// FlowExecutions into one status. The aggregate status will be the
        /// status with the highest precedence.
        /// </summary>
        /// <param name="executions"></param>
        /// <returns></returns>
        public FlowExecutionStatus Aggregate(ICollection<FlowExecution> executions)
        {
            if (executions == null || executions.Count == 0)
            {
                return FlowExecutionStatus.Unkown;
            }
            return executions.Max().Status;
        }
    }
}