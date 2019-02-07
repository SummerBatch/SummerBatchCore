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

namespace Summer.Batch.Core.Unity.Injection
{
    /// <summary>
    /// Implementation of <see cref="IDependencyResolverPolicy"/> that concats strings that are
    /// evaluated at runtime. Values can be of type <see cref="IDependencyResolverPolicy"/>,
    /// <see cref="Lazy{T}"/>, or <see cref="Func{T}"/>. Values of other type are converted to string.
    /// </summary>
    public class StringConcatenationResolverPolicy : IDependencyResolverPolicy
    {
        private readonly dynamic[] _values;

        /// <summary>
        /// Constructs a new <see cref="StringConcatenationResolverPolicy"/>.
        /// </summary>
        /// <param name="values">the values to evaluate and concatenate at resolution.</param>
        public StringConcatenationResolverPolicy(dynamic[] values)
        {
            _values = values;
        }

        /// <summary>
        /// Resolve object from the given context.
        /// </summary>
        /// <param name="context"></param>
        /// <returns></returns>
        public object Resolve(IBuilderContext context)
        {
            if (_values.Length == 0)
            {
                return null;
            }
            string result = GetValue(context, _values[0]);
            for (var i = 1; i < _values.Length; i++)
            {
                result += GetValue(context, _values[i]);
            }
            return result;
        }

        #region GetValue overloads

        private static string GetValue(IBuilderContext context, object obj)
        {
            return obj.ToString();
        }

        private static string GetValue(IBuilderContext context, string s)
        {
            return s;
        }

        private static string GetValue<T>(IBuilderContext context, Lazy<T> lazy)
        {
            return lazy.Value.ToString();
        }

        private static string GetValue(IBuilderContext context, IDependencyResolverPolicy resolverPolicy)
        {
            return resolverPolicy.Resolve(context).ToString();
        }

        private static string GetValue<T>(IBuilderContext context, Func<T> func)
        {
            return func().ToString();
        }

        #endregion
    }
}