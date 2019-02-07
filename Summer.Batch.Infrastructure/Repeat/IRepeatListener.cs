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
namespace Summer.Batch.Infrastructure.Repeat
{
    /// <summary>
    /// Interface for listeners to the batch process. Implementers can provide
    /// enhance the behaviour of a batch in small cross-cutting modules. The
    /// framework provides callbacks at key points in the processing.
    /// </summary>
    public interface IRepeatListener
    {
        /// <summary>
        ///  Called by the framework before each batch item. Implementers can halt a
        /// batch by setting the complete flag on the context.
        /// </summary>
        /// <param name="context"></param>
        void Before(IRepeatContext context);

        /// <summary>
        /// Called by the framework after each item has been processed, unless the
        /// item processing results in an exception. This method is called as soon as
        /// the result is known.
        /// </summary>
        /// <param name="context"></param>
        /// <param name="result"></param>
        void After(IRepeatContext context, RepeatStatus result);

        /// <summary>
        ///Called once at the start of a complete batch, before any items are
        /// processed. Implementers can use this method to acquire any resources that
        /// might be needed during processing. Implementers can halt the current
        /// operation by setting the complete flag on the context. To halt all
        /// enclosing batches (the whole job), the would need to use the parent
        /// context (recursively). 
        /// </summary>
        /// <param name="context"></param>
        void Open(IRepeatContext context);

        /// <summary>
        ///  Called when a repeat callback fails by throwing an exception. There will
        ///be one call to this method for each exception thrown during a repeat
        /// operation (e.g. a chunk).
        /// There is no need to re-throw the exception here - that will be done by
        /// the enclosing framework.
        /// </summary>
        /// <param name="context"></param>
        /// <param name="e"></param>
        void OnError(IRepeatContext context, System.Exception e);

        /// <summary>
        /// Called once at the end of a complete batch, after normal or abnormal
        /// completion (i.e. even after an exception). Implementers can use this
        /// method to clean up any resources.
        /// </summary>
        /// <param name="context"></param>
        void Close(IRepeatContext context);
    }
}