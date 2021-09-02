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
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Practices.Unity;
using NLog;
using Summer.Batch.Common.Transaction;
using Summer.Batch.Core;
using Summer.Batch.Data;

namespace Summer.Batch.Extra
{
    /// <summary>
    /// Common implementation of the pre processor, processor and post processor.
    /// Manages step and job contexts.
    /// </summary>
    public class AbstractExecutionListener : IStepExecutionListener
    {

        private const string Restart = "batch.restart";
        private const string PreProcessor = "batch.preprocessor";
        private const string semicolon = ";";
        private const string dot = ".";
        private static readonly Logger Logger = LogManager.GetCurrentClassLogger();

        /// <summary>
        /// The context manager for the job context.
        /// </summary>
        [Dependency(BatchConstants.JobContextManagerName)]
        public IContextManager JobContextManager { private get; set; }

        /// <summary>
        /// The context manager for the step context.
        /// </summary>
        [Dependency(BatchConstants.StepContextManagerName)]
        public IContextManager StepContextManager { private get; set; }

        /// <summary>
        /// Default implementation of preprocessing will do nothing.
        /// If a pre processor exists, subclass will override this method.
        /// </summary>
        protected virtual void Preprocess()
        {
            //does nothing on purpose
        }

        /// <summary>
        /// Default implementation of postprocessing will only return COMPLETED code.
        /// If a post processor exists, subclass will override this method.
        /// </summary>
        /// <returns></returns>
        protected virtual ExitStatus Postprocess()
        {
            return ExitStatus.Completed;
        }

        /// <summary>
        /// Logic launched before the step. Will register the contexts and launch the preprocessor.
        /// @see IStepExecutionListener#BeforeStep
        /// </summary>
        /// <param name="stepExecution"></param>
        public virtual void BeforeStep(StepExecution stepExecution)
        {
            RegisterContexts(stepExecution);

            
            if (stepExecution.remoteChunking != null)
            {
                AutoResetEvent threadWait = new AutoResetEvent(false);

                // master step create control thread 
                if (stepExecution.remoteChunking._master)
                {
                    // controlthread with callback method 
                    Thread thread = new Thread((ThreadStart)(() => Master(stepExecution, threadWait)));
                    stepExecution.remoteChunking.controlThread = thread;
                    stepExecution.remoteChunking.threadWait = threadWait;
                    thread.Start();
                    TimeSpan _maxMasterWaitWorkerSecond = TimeSpan.FromSeconds(stepExecution.remoteChunking.MaxMasterWaitWorkerSecond);
                    int _maxMasterWaitWorkerRetry = stepExecution.remoteChunking.MaxMasterWaitWorkerRetry;
                    // wait for Master method send back workerStarted signal
                    if (threadWait.WaitOne(_maxMasterWaitWorkerSecond * _maxMasterWaitWorkerRetry))
                    {
                        Logger.Info("Worker is started.");
                    }
                    else
                    {
                        // clean all message queues when no work job provided
                        Exception e = new JobExecutionException("No worker job provided");
                        Logger.Error(e, "Clean message in the Queue.");
                        stepExecution.remoteChunking.CleanAllQueue();
                        throw e;
                    }
                }
                else // worker step create control thread 
                {
                    Thread thread = new Thread((ThreadStart)(() => Worker(stepExecution, threadWait)));
                    stepExecution.remoteChunking.controlThread = thread;
                    stepExecution.remoteChunking.threadWait = threadWait;
                    thread.Start();
                }

              
            }
            if (!stepExecution.ExecutionContext.ContainsKey(PreProcessor))
            {
                stepExecution.ExecutionContext.Put(PreProcessor, true);
            }

            if (!stepExecution.ExecutionContext.ContainsKey(Restart) || (stepExecution.ExecutionContext.ContainsKey(PreProcessor) && (bool)stepExecution.ExecutionContext.Get(PreProcessor)))
            {
                try
                {
                    Preprocess();
                    stepExecution.ExecutionContext.Put(PreProcessor, false);
                }
                catch (Exception e)
                {
                    throw;
                }
            }
        }

        /// <summary>
        /// Logic launched after the step. Will launch the postprocessor.
        /// @see IStepExecutionListener#AfterStep
        /// </summary>
        /// <param name="stepExecution"></param>
        /// <returns></returns>
        public virtual ExitStatus AfterStep(StepExecution stepExecution)
        {

            ExitStatus returnStatus = stepExecution.ExitStatus;
            if (!"FAILED".Equals(returnStatus.ExitCode))
            {
                MethodInfo post = GetType().GetMethod("Postprocess", BindingFlags.Instance | BindingFlags.NonPublic);
                // determine worker / master mode for step 
                if (stepExecution.remoteChunking != null)
                {
                    TimeSpan RemoteChunkingTimoutSecond = stepExecution.remoteChunking.RemoteChunkingTimoutSecond;
                    // master part 
                    if (stepExecution.remoteChunking._master)
                    {
                        // wait for Master method send back signal
                        if (!stepExecution.remoteChunking.threadWait.WaitOne(-1))
                        {
                            Exception e = new JobExecutionException("Master step failed");
                            Logger.Error(e, "Clean message in the Queue.");
                            throw e;
                        }
                        // some worker failed master need to fail 
                        if ("FAILED".Equals(stepExecution.ExitStatus.ExitCode))
                        {
                            Exception e = new JobExecutionException("Master step failed");
                            Logger.Error(e, "Clean message in the Queue.");
                            throw e;
                        }

                        // clean all message queues when master completed 
                        Logger.Info("Master is completed.");
                        Logger.Info("Clean message in the Queue.");
                        stepExecution.remoteChunking.CleanAllQueue();
                    }
                    else
                    {
                        if (!stepExecution.remoteChunking.threadWait.WaitOne(-1))
                        {
                            returnStatus = stepExecution.ExitStatus;
                            throw new JobExecutionException("Worker step failed");
                        }

                        Logger.Info("Worker is completed.");
                    }

                }


                if (post.DeclaringType != typeof(AbstractExecutionListener))
                {
                    using (var scope = TransactionScopeManager.CreateScope())
                    {
                        try
                        {

                            returnStatus = Postprocess();
                        }
                        catch (Exception e)
                        {
                            // Need to catch exception to log and set status to FAILED, while
                            // Spring batch would only log and keep the status COMPLETED
                            Logger.Error(e, "Exception during postprocessor");
                            stepExecution.UpgradeStatus(BatchStatus.Failed);
                            throw;
                        }
                        scope.Complete();
                    }
                }
            } 
            // batch failed need to join the thread
            else if (stepExecution.remoteChunking != null) 
            {
                stepExecution.remoteChunking.controlThread.Join();
            }

            return returnStatus;
        }

        /// <summary>
        ///  Wait for all worker completed before master completed.
        ///  if worker failed , it will add to failList and return to the master.
        /// </summary>
        /// <param name="stepExecution"></param>
        /// <param name="workerMaxNumber"></param>
        /// <param name="maxReTry"></param>
        /// <param name="waitTime"></param>
        private List<string> WaitForWorkerCompleted(StepExecution stepExecution, int workerMaxNumber, int maxReTry, TimeSpan waitTime)
        {
            List<string> workerIDList = new List<string>(stepExecution.remoteChunking._workerMap.Keys);
            int workerIDcount = workerIDList.Count;

            List<string> workerFailList = new List<string>();
            List<string> workerCompletedList = new List<string>();

            // check worker existed and complete all of them 
            if (workerIDcount > 0 && workerIDcount <= workerMaxNumber)
            {
                while (workerIDcount > 0)
                {
                    // wait for worker to complete 
                    Thread.Sleep(waitTime);

                    // update completed worker
                    Dictionary<string, bool> workerMap = stepExecution.remoteChunking._workerMap;
                    List<string> workerIDCurrentList = new List<string>(workerMap.Keys);
                    workerIDList.AddRange(new List<string>(workerIDCurrentList));
                    workerIDList = workerIDList.Distinct<string>().ToList<string>();
                    workerCompletedList.AddRange(UpdateWorkerCompletedList(stepExecution, workerIDList));

                    // update faillist and completed worker regularly
                    Tuple<List<string>, List<string>> tuple = UpdateWorkerIDList(workerMap, stepExecution, workerIDList, workerFailList, workerCompletedList, maxReTry, waitTime);
                    int count = tuple.Item1.Count;
                    workerFailList = tuple.Item2;

                    // all workers are finished
                    if (count == 0)
                    {
                        Logger.Info("No worker need to wait .");
                        break;
                    }
                    Logger.Info("{0} worker need to complete", count);
                }
            }

            // clean message queue when all salves are completed
            Logger.Info("Clean message in the Queue.");
            stepExecution.remoteChunking.CleanAllQueue();
            return workerFailList;
        }

        /// <summary>
        /// Update WorkerCompletedList and WorkerFailList.
        /// </summary>
        /// <param name="workerMap"></param>
        /// <param name="stepExecution"></param>
        /// <param name="workerStartedIDs"></param>
        /// <param name="prevfailList"></param>
        /// <param name="workerCompletedList"></param>
        /// <param name="maxReTry"></param>
        /// <param name="waitRetryTime"></param>
        /// <returns></returns>
        private Tuple<List<string>, List<string>> UpdateWorkerIDList(Dictionary<string, bool> workerMap, StepExecution stepExecution, List<string> workerStartedIDs, List<string> workerFailList, List<string> workerCompletedList, int maxReTry, TimeSpan waitRetryTime)
        {
            List<string> deleteList = new List<string>();
            List<string> currentWorkerFailList = new List<string>();

            // get currentWorkerFailList from workerMap in stepExecution
            foreach (string workerStartedId in workerStartedIDs)
            {
                if (!workerMap[workerStartedId])
                {
                    if (!workerCompletedList.Contains(workerStartedId))
                    {
                        currentWorkerFailList.Add(workerStartedId);
                    }
                    deleteList.Add(workerStartedId);
                }
            }

            // compare with previousWorkrerfailList
            if (currentWorkerFailList.Count != workerFailList.Count)
            {
                // check worker is alive or not
                while (maxReTry > 0)
                {
                    Thread.Sleep(waitRetryTime);
                    Logger.Info("Retry to check worker is alive");
                    Dictionary<string, bool> workerCurrentMap = stepExecution.remoteChunking._workerMap;
                    foreach (string failedID in currentWorkerFailList)
                    {
                        if (workerCurrentMap[failedID])
                        {
                            deleteList.Remove(failedID);
                        }
                        else
                        {
                            deleteList.Add(failedID);
                        }
                    }
                    currentWorkerFailList.RemoveAll((ID => !deleteList.Contains(ID)));
                    maxReTry--;
                }

                // update WorkerFailList with currentWorkerFailList
                workerFailList = new List<string>(currentWorkerFailList);
            }

            // update WorkerFailList with workerCompletedList
            workerCompletedList.AddRange(UpdateWorkerCompletedList(stepExecution, workerStartedIDs));
            workerFailList.RemoveAll((item => workerCompletedList.Contains(item)));

            // remove duplicate workerID in the list
            deleteList = deleteList.Distinct<string>().ToList<string>();
            workerStartedIDs.RemoveAll((item => deleteList.Contains(item)));
            return Tuple.Create<List<string>, List<string>>(workerStartedIDs, workerFailList);
        }

        /// <summary>
        /// Update WorkerCompletedList.
        /// </summary>
        /// <param name="stepExecution"></param>
        /// <param name="workerStartedIDs"></param>
        /// <returns></returns>
        private List<string> UpdateWorkerCompletedList(StepExecution stepExecution, List<string> workerStartedIDs)
        {
            List<string> workerCompleteIDs = new List<string>();
            foreach (string workerStartedId in workerStartedIDs)
            {
                string targetmessage = workerStartedId + dot + "COMPLETED";
                if (stepExecution.remoteChunking._workerCompletedQueue.CheckMessageExistAndConsume(targetmessage))
                {
                    workerCompleteIDs.Add(workerStartedId);
                }
                   
            }
            return workerCompleteIDs;
        }

        /// <summary>
        /// Wait at least one worker started.
        /// </summary>
        /// <param name="stepExecution"></param>
        /// <param name="maxReTry"></param>
        /// <param name="waitTime"></param>
        /// <returns></returns>
        private bool WaitForAtLeastWorkerStarted(StepExecution stepExecution, int maxRetry, TimeSpan waitTime)
        {
            List<string> workerStartedIDs = stepExecution.remoteChunking._workerStartedQueue.GetWorkerIDByMasterName(stepExecution.StepName);

            //// set waitTime to 1 second and maxRetry to totalSecond
            int totalWaitTime = (int)waitTime.TotalSeconds;
            maxRetry = totalWaitTime * maxRetry;
            waitTime = TimeSpan.FromSeconds(1);
            if (workerStartedIDs.Count == 0)
            {
                // check at least one worker started
                while(maxRetry > 0)
                {
                    Logger.Info("Wait for worker {0} seconds.", waitTime.TotalSeconds);
                    Thread.Sleep(waitTime);
                    workerStartedIDs = stepExecution.remoteChunking._workerStartedQueue.GetWorkerIDByMasterName(stepExecution.StepName);
                    if (workerStartedIDs.Count > 0)
                    {
                        Dictionary<string, bool> workerCurrentMap = stepExecution.remoteChunking._workerMap;
                        foreach (string key in workerStartedIDs)
                        {
                            if (!workerCurrentMap.ContainsKey(key))
                            {
                                workerCurrentMap[key] = true;
                            }
                        }
                        stepExecution.remoteChunking._workerMap = workerCurrentMap;
                        return true;
                    }
                    maxRetry--;
                }
                return false;
            }

            // update workerMap in the stepExecution
            Dictionary<string, bool> workerMap = stepExecution.remoteChunking._workerMap;
            foreach (string key in workerStartedIDs)
            {
                if (!workerMap.ContainsKey(key))
                {
                    workerMap[key] = true;
                }
            }
            stepExecution.remoteChunking._workerMap = workerMap;
            return true;
        }

        /// <summary>
        /// Launch Master method in the controlThread.
        /// </summary>
        /// <param name="stepExecution"></param>
        /// <param name="threadWait"></param>
        public void Master(StepExecution stepExecution, AutoResetEvent threadWait)
        {
            // clean up all message queues
            stepExecution.remoteChunking.CleanAllQueue();

            // get workerxml name
            string WorkerXml = stepExecution.remoteChunking.WorkerFileName;

            // get worker max numbers
            int workerMaxNumber = stepExecution.remoteChunking.WorkerMaxNumber;

            // configuration information includes stepname , workerxml name, and worker max numbers
            string message = stepExecution.StepName + semicolon + WorkerXml + semicolon + workerMaxNumber.ToString();

            // master send configuration information in the control queue for worker job to execute
            stepExecution.remoteChunking._controlQueue.Send(message);
            int maxMasterWaitWorkerSecond = stepExecution.remoteChunking.MaxMasterWaitWorkerSecond;
            int maxMasterWaitWorkerRetry = stepExecution.remoteChunking.MaxMasterWaitWorkerRetry;
            TimeSpan _MasterWaitWorkerTimeout = TimeSpan.FromSeconds(maxMasterWaitWorkerSecond);
            // check at least one worker started
            if (WaitForAtLeastWorkerStarted(stepExecution, maxMasterWaitWorkerRetry, _MasterWaitWorkerTimeout))
            {
                // send back signal to the beforeStep
                threadWait.Set();
                bool Isterminate = false;

                int _masterTimerseconds = maxMasterWaitWorkerSecond * 1000;
                // start a timer to check worker is still alive or not in every defualt seconds
                Timer timer = new Timer(MasterTimerMethod, stepExecution, 0, _masterTimerseconds);
                while (!Isterminate)
                {
                    ControlQueue masterQueue = stepExecution.remoteChunking._masterQueue;

                    // send back signal to the afterStep and stop timer when batch failed
                    if ("FAILED".Equals(stepExecution.ExitStatus.ExitCode))
                    {
                        Isterminate = true;
                        timer.Dispose();
                        threadWait.Set();
                    }
                    // when batch completed send back signal to the afterStep and stop timer after send completedmessage to message queue and check all workers completed
                    else if ("COMPLETED".Equals(stepExecution.ExitStatus.ExitCode))
                    {
                        string masterCompletedMessage = "master"+ dot + stepExecution.StepName + dot + stepExecution.ExitStatus.ExitCode;
                        int maxWorkerCompleteMessageNeeded = workerMaxNumber;
                        while (maxWorkerCompleteMessageNeeded > 0)
                        {
                            masterQueue.Send(masterCompletedMessage);
                            maxWorkerCompleteMessageNeeded--;
                        }
                        List<string> failList = WaitForWorkerCompleted(stepExecution, workerMaxNumber, maxMasterWaitWorkerSecond, _MasterWaitWorkerTimeout);
                        timer.Dispose();
                        if (failList.Count > 0)
                        {
                            stepExecution.ExitStatus = ExitStatus.Failed;
                        }
                        threadWait.Set();
                        Isterminate = true;
                    }
                }
            }
        }

        /// <summary>
        /// Launch Master timer method in the controlThread.
        /// </summary>
        /// <param name="stepExectuionObject"></param>
        private void MasterTimerMethod(object stepExectuionObject)
        {
            StepExecution stepExecution = (StepExecution)stepExectuionObject;
            Logger.Debug("-------------------------------------------- Master Timer --------------------------------------------");
            string stepName = stepExecution.StepName;
            ControlQueue masterLifeLineQueue = stepExecution.remoteChunking._masterLifeLineQueue;
            string masterNoAliveMessage = stepName + dot + bool.FalseString;
            // stop timer when batch failed
            if ("FAILED".Equals(stepExecution.ExitStatus.ExitCode))
            {
                masterLifeLineQueue.Send(masterNoAliveMessage);
                return;
            }
            else // continue update workerMap in the stepExecution and check workers alive
            {
                Dictionary<string, bool> workerMap = stepExecution.remoteChunking._workerMap;
                List<string> workerIDList = stepExecution.remoteChunking._workerStartedQueue.GetWorkerIDByMasterName(stepExecution.StepName);
                foreach (string workerID in workerIDList)
                {
                    if (!workerMap.ContainsKey(workerID))
                    {
                        workerMap[workerID] = true;
                    }
                }
                ControlQueue workerLifeLineQueue = stepExecution.remoteChunking._workerLifeLineQueue;
                List<string> workerIDs = new List<string>(workerMap.Keys);
                workerLifeLineQueue.CheckMessageExistAndConsumeAll(workerIDs, workerMap);
            }
        }

        /// <summary>
        /// Launch Worker method in the controlThread.
        /// </summary>
        /// <param name="stepExecution"></param>
        /// <param name="threadWait"></param>
        public void Worker(StepExecution stepExecution, AutoResetEvent threadWait)
        {

            // workerStartedMessage includes stepname and worker id
            string workerStartedMessage = stepExecution.StepName + dot + stepExecution.remoteChunking.WorkerID.ToString();

            // worker send workerStartedMessage in the workerStarted queue for master job to check
            stepExecution.remoteChunking._workerStartedQueue.Send(workerStartedMessage);
            int maxMasterWaitWorkerSecond = stepExecution.remoteChunking.MaxMasterWaitWorkerSecond;
            int _workerTimerseconds = maxMasterWaitWorkerSecond * 1000;
            // start a timer to let master check worker is still alive or not in every defualt seconds
            Timer timer = new Timer(WorkerTimerMethod, stepExecution, 0, _workerTimerseconds);
            bool Isterminate = false;
            while (!Isterminate)
            {
                // send back signal to the afterStep and stop timer when batch failed
                ControlQueue workerCompletedQueue = stepExecution.remoteChunking._workerCompletedQueue;
                if ("FAILED".Equals(stepExecution.ExitStatus.ExitCode))
                {
                    timer.Dispose();
                    Isterminate = true;
                }
                // when batch completed send back signal to the afterStep and stop timer after send completedmessage to message queue 
                else if ("COMPLETED".Equals(stepExecution.ExitStatus.ExitCode))
                {
                    string workerCompletedMessage = stepExecution.JobExecution.JobInstance.JobName + dot + stepExecution.remoteChunking.WorkerID + dot + "COMPLETED";
                    workerCompletedQueue.Send(workerCompletedMessage);
                    timer.Dispose();
                    threadWait.Set();
                    Isterminate = true;
                }
            }
        }

        /// <summary>
        /// Launch Worker timer method in the controlThread.
        /// </summary>
        /// <param name="stepExectuionObject"></param>
        private void WorkerTimerMethod(object stepExectuionObject)
        {
            StepExecution stepExecution = (StepExecution)stepExectuionObject;
            string workerIdMessage = stepExecution.StepName + dot + stepExecution.remoteChunking.WorkerID.ToString();
            Logger.Info("-------------------------------------------- Worker Timer --------------------------------------------");
            ControlQueue workerLifeLineQueue = stepExecution.remoteChunking._workerLifeLineQueue;
            string workerAliveMessage = workerIdMessage + dot + bool.TrueString + dot + DateTime.Now.ToString();
            string workerNoAliveMessage = workerIdMessage + dot + bool.FalseString + dot + DateTime.Now.ToString();

            // stop timer when batch failed
            if ("FAILED".Equals(stepExecution.ExitStatus.ExitCode))
            {
                workerLifeLineQueue.Send(workerNoAliveMessage);
                Logger.Debug(workerIdMessage + "-----------NoAlive------------------");
                return;
            }
            else // continue send workerAliveMessage to the workerLifeLineQueue 
            {
                Logger.Debug(workerIdMessage + "-----------Alive------------------");
                workerLifeLineQueue.Send(workerAliveMessage);
            }
        }

        /// <summary>
        /// registers both job and step contexts.
        /// </summary>
        /// <param name="stepExecution"></param>
        private void RegisterContexts(StepExecution stepExecution)
        {
            JobExecution jobExecution = stepExecution.JobExecution;
            JobContextManager.Context = jobExecution.ExecutionContext;
            StepContextManager.Context = stepExecution.ExecutionContext;
        }


        /// <summary>
        /// Checks if the record is the last one.
        /// </summary>
        /// <param name="arg">the record to check</param>
        /// <returns>whether the record is the last one</returns>
        protected bool IsLast(object arg)
        {
            bool isLast = false;
            if (StepContextManager.ContainsKey(BatchConstants.LastRecordKey))
            {
                isLast = StepContextManager.GetFromContext(BatchConstants.LastRecordKey).Equals(arg);
            }
            return isLast;
        }

    }
}