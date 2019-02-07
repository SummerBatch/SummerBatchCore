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
using Microsoft.Practices.ObjectBuilder2;
using Summer.Batch.Common.Util;
using Summer.Batch.Core.Scope.Context;

namespace Summer.Batch.Core.Unity.Injection
{
    /// <summary>
    /// Dependency resolver that reads a property from the job context.
    /// </summary>
    public class JobContextDependencyResolverPolicy<T> : IDependencyResolverPolicy
    {
        private readonly string _propertyName;

        /// <summary>
        /// Constructs a new JobContextDependencyResolverPolicy.
        /// </summary>
        /// <param name="propertyName">the name of the property to read</param>
        public JobContextDependencyResolverPolicy(string propertyName)
        {
            _propertyName = propertyName;
        }

        /// <summary>
        /// Resolved object from given context.
        /// </summary>
        /// <param name="context"></param>
        /// <returns></returns>
        public object Resolve(IBuilderContext context)
        {
            object value;
            StepSynchronizationManager.GetContext().GetJobExecutionContext().TryGetValue(_propertyName, out value);
            if (value == null)
            {
                throw new InvalidOperationException(string.Format("Failed to get property {0} in step context.", _propertyName));
            }
            return value is T ? (T)value : StringConverter.Convert<T>(value.ToString());
        }
    }
}