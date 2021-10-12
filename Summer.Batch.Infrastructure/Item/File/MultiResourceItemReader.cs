//
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

// This file has been modified.
// Original copyright notice :

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
using System;
using System.Collections.Generic;
using NLog;
using Summer.Batch.Common.IO;
using Summer.Batch.Infrastructure.Item.Util;
using Summer.Batch.Common.Util;

namespace Summer.Batch.Infrastructure.Item.File
{
    /// <summary>
    /// Reads items from a collection of resources sequentially. Ordering of resources is preserved between jobs runs
    /// (restartability support) using the provided comparer.
    /// </summary>
    /// <typeparam name="T">&nbsp;</typeparam>
    public class MultiResourceItemReader<T> : ItemStreamSupport, IItemStreamReader<T> where T : class
    {
        private const string ResourceKey = "resourceIndex";
        private const string ResourceMap = "batch.resourcesMap";
        private readonly Logger _logger = LogManager.GetCurrentClassLogger();
        private ExecutionContext context;
        private int _currentResource = -1;

        private bool _noInput;
        private Dictionary<string, IList<T>> _resourcesMap;
        /// <summary>
        /// Delegate stream.
        /// </summary>
        public IResourceAwareItemReaderItemStream<T> Delegate { get; set; }

        /// <summary>
        /// Collection of used resources.
        /// </summary>
        public IList<IResource> Resources { get; set; }

        /// <summary>
        /// Save state.
        /// </summary>
        public bool SaveState { get; set; }

        /// <summary>
        /// Comparer.
        /// </summary>
        public IComparer<IResource> Comparer { get; set; }

        /// <summary>
        /// Strict mode indicator.
        /// </summary>
        public bool Strict { get; set; }

        /// <summary>
        /// Returns the current resource.
        /// </summary>
        public IResource CurrentResource
        {
            get { return _currentResource < 0 || _currentResource >= Resources.Count ? null : Resources[_currentResource]; }
        }

        /// <summary>
        /// Default constructor.
        /// </summary>
        public MultiResourceItemReader()
        {
            Name = typeof (MultiResourceItemReader<T>).Name;
            Comparer = new DefaultComparer();
            SaveState = true;
        }

        /// <summary>
        /// Open resources.
        /// </summary>
        /// <param name="executionContext"></param>
        public override void Open(ExecutionContext executionContext)
        {
            Assert.NotNull(Resources, "resources must be set");

            if (Resources.Count == 0)
            {
                if (Strict)
                {
                    throw new InvalidOperationException("No resources to read in strict mode.");
                }
                _logger.Warn("No resources to read.");
                _noInput = true;
                return;
            }

            IResource[] resources = new List<IResource>(Resources).ToArray();
            Array.Sort(resources, Comparer);

            if (executionContext.ContainsKey(GetExecutionContextKey(ResourceKey)))
            {
                _currentResource = executionContext.GetInt(GetExecutionContextKey(ResourceKey));
                if (_currentResource == -1)
                {
                    _currentResource = 0;
                }
                Delegate.Resource = Resources[_currentResource];
                Delegate.Open(executionContext);
                context = executionContext;
                if (context.ContainsKey(ResourceMap))
                {
                    _resourcesMap = (Dictionary<string, IList<T>>)context.Get(ResourceMap);
                }
                else
                {
                    _resourcesMap = new Dictionary<string, IList<T>>();
                }

            }
            else
            {
                _resourcesMap = new Dictionary<string, IList<T>>();
                _currentResource = -1;
            }
        }

        /// <summary>
        /// Delegates to delegate stream.
        /// </summary>
        public override void Close()
        {
            Delegate.Close();
            _noInput = false;
        }

        /// <summary>
        /// Delegates to delegate stream.
        /// </summary>
        /// <param name="executionContext"></param>
        public override void Update(ExecutionContext executionContext)
        {
            if (SaveState)
            {
                context = executionContext;
                executionContext.PutInt(GetExecutionContextKey(ResourceKey), _currentResource);
                Delegate.Update(executionContext);
            }
        }

        /// <summary>
        /// Delegates to delegate stream.
        /// </summary>
        public override void Flush()
        {
            Delegate.Flush();
        }

        /// <summary>
        /// Read from current resource.
        /// </summary>
        /// <returns></returns>
        public T Read()
        {
            if (_noInput)
            {
                return null;
            }

            if (_currentResource == -1)
            {
                _currentResource = 0;
                Delegate.Resource = Resources[_currentResource];
                Delegate.Open(new ExecutionContext());
            }


            return ReadNextItem();
        }

        /// <summary>
        /// Read next item.
        /// </summary>
        /// <returns></returns>
        private T ReadNextItem()
        {
            var item = Delegate.Read();

            if (item != null)
            {
                if (CurrentResource != null)
                {
                    string fileName = CurrentResource.GetFileInfo().Name;
                    context.Put(ResourceMap, _resourcesMap);
                    if (_resourcesMap.ContainsKey(fileName))
                    {
                        _resourcesMap[fileName].Add(item);
                    }
                    else
                    {
                        List<T> objectList = new List<T>();
                        objectList.Add(item);
                        _resourcesMap.Add(fileName, objectList);
                    }
                    context.Put(ResourceMap, _resourcesMap);
                }
            }

            while (item == null)
            {
                _currentResource++;

                if (_currentResource >= Resources.Count)
                {
                    break;
                }

                Delegate.Close();
                Delegate.Resource = Resources[_currentResource];
                Delegate.Open(new ExecutionContext());
                item = Delegate.Read();
                if (item != null)
                {
                    if (CurrentResource != null)
                    {
                        string fileName = CurrentResource.GetFileInfo().Name;
                        if (_resourcesMap.ContainsKey(fileName))
                        {
                            _resourcesMap[fileName].Add(item);
                        }
                        else
                        {
                            List<T> objectList = new List<T>();
                            objectList.Add(item);
                            _resourcesMap.Add(fileName, objectList);
                        }
                        context.Put(ResourceMap, _resourcesMap);
                    }
                }
            }

            base.Update(context);
            return item;
        }

        private class DefaultComparer : IComparer<IResource>
        {
            public int Compare(IResource x, IResource y)
            {
                return string.Compare(x.GetFilename(), y.GetFilename(), StringComparison.Ordinal);
            }
        }
    }
}