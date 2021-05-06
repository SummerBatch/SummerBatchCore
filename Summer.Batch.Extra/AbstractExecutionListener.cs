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

        // need to configure in the remotechunking throught xml
        private const string SlaveXml = "Slave.xml";
        private const int slaveMaxNumber = 2;

        private const int maxSlaveReTry = 3;
        private int _maxMasterWaitSlaveStartedRetry = 10;
        private int _maxMasterwaitSlaveCompletedRetry = 3;
        private TimeSpan _MasterwaitSlaveCompletedTime = TimeSpan.FromSeconds(6);
        private TimeSpan _MasterWaitSlaveStartedtimeout = TimeSpan.FromSeconds(1);
        private TimeSpan _waitRetryTime = TimeSpan.FromSeconds(3);
        private const int _masterTimerseconds = 5000;
        private const int _slaveTimerseconds = 5000;
        private TimeSpan _beforeStepThreadTimeout = TimeSpan.FromSeconds(15);
        private TimeSpan _afterStepThreadTimeout = TimeSpan.FromMinutes(15);

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
                    Thread thread = new Thread((ThreadStart)(() => this.Master(stepExecution, threadWait)));
                    stepExecution.remoteChunking.controlThread = thread;
                    stepExecution.remoteChunking.threadWait = threadWait;
                    thread.Start();
                    if (threadWait.WaitOne(this._beforeStepThreadTimeout))
                    {
                        Logger.Info("Slave is started.");
                    }
                    else
                    {
                        Logger.Info("Clean message in the Queue.");
                        stepExecution.remoteChunking.CleanAllQueue();
                        throw new JobExecutionException("No slave job provided");
                    }
                }
                else
                {
                    Thread thread = new Thread((ThreadStart)(() => this.Slave(stepExecution, threadWait)));
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
                MethodInfo post = this.GetType().GetMethod("Postprocess", BindingFlags.Instance | BindingFlags.NonPublic);
                // determine slave / master mode for step 
                if (stepExecution.remoteChunking != null)
                {

                    if (stepExecution.remoteChunking._master)
                    {
                        if (!stepExecution.remoteChunking.threadWait.WaitOne(_afterStepThreadTimeout))
                        {
                            throw new JobExecutionException("Master step failed");
                        }
                        // some slave failed
                        if ("FAILED".Equals(stepExecution.ExitStatus.ExitCode))
                        {
                            throw new JobExecutionException("Master step failed");
                        }

                        Logger.Info("Master is completed.");
                        Logger.Info("Clean message in the Queue.");
                        stepExecution.remoteChunking.CleanAllQueue();
                    }
                    else
                    {
                        if (!stepExecution.remoteChunking.threadWait.WaitOne(_afterStepThreadTimeout))
                        {
                            returnStatus = stepExecution.ExitStatus;
                            throw new JobExecutionException("Master step failed");
                        }

                        Logger.Info("Slave is completed.");
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
            else if (stepExecution.remoteChunking != null)
            {
                stepExecution.remoteChunking.controlThread.Join();
            }

            return returnStatus;
        }

        /// <summary>
        ///  Wait for all slave completed before master completed.
        ///  if slave failed , it will add to failList and return to the master.
        /// </summary>
        /// <param name="stepExecution"></param>
        /// <param name="slaveMaxNumber"></param>
        /// <param name="maxReTry"></param>
        /// <param name="waitTime"></param>
        private List<string> WaitForSlaveCompleted(StepExecution stepExecution, int slaveMaxNumber, int maxReTry, TimeSpan waitTime)
        {
            List<string> slaveIDList = new List<string>(stepExecution.remoteChunking._slaveMap.Keys);
            int slaveIDcount = slaveIDList.Count;

            List<string> prevfailList = new List<string>();
            List<string> slaveCompletedList = new List<string>();

            if (slaveIDcount > 0 && slaveIDcount <= slaveMaxNumber)
            {
                while (slaveIDcount > 0)
                {
                    Thread.Sleep(waitTime);
                    Dictionary<string, bool> slaveMap = stepExecution.remoteChunking._slaveMap;
                    List<string> slaveIDCurrentList = new List<string>(slaveMap.Keys);
                    slaveIDList.AddRange(new List<string>(slaveIDCurrentList));
                    slaveIDList = slaveIDList.Distinct<string>().ToList<string>();
                    slaveCompletedList.AddRange(UpdateSlaveCompletedList(stepExecution, slaveIDList));
                    Tuple<List<string>, List<string>> tuple = UpdateSlaveIDList(slaveMap, stepExecution, slaveIDList, prevfailList, slaveCompletedList, maxReTry, _waitRetryTime);
                    int count = tuple.Item1.Count;
                    prevfailList = tuple.Item2;
                    // all slave is finished
                    if (count == 0)
                    {
                        Logger.Info("No slave need to wait .");
                        break;
                    }
                    Logger.Info("{0} slave need to complete", count);
                }
            }


            Logger.Info("Clean message in the Queue.");
            stepExecution.remoteChunking.CleanAllQueue();
            return prevfailList;
        }


        private Tuple<List<string>, List<string>> UpdateSlaveIDList(Dictionary<string, bool> slaveMap, StepExecution stepExecution, List<string> slaveStartedIDs, List<string> prevfailList, List<string> slaveCompletedList, int maxReTry, TimeSpan waitRetryTime)
        {
            List<string> deleteList = new List<string>();
            List<string> currentfailList = new List<string>();
            foreach (string slaveStartedId in slaveStartedIDs)
            {
                if (!slaveMap[slaveStartedId])
                {
                    if (!slaveCompletedList.Contains(slaveStartedId))
                    {
                        currentfailList.Add(slaveStartedId);
                    }
                    deleteList.Add(slaveStartedId);
                }
            }
            if (currentfailList.Count != prevfailList.Count)
            {
                while (maxReTry > 0)
                {
                    Thread.Sleep(waitRetryTime);
                    Logger.Info("Retry to make sure slave is alive");
                    Dictionary<string, bool> slaveCurrentMap = stepExecution.remoteChunking._slaveMap;
                    foreach (string failedID in currentfailList)
                    {
                        if (slaveCurrentMap[failedID])
                        {
                            deleteList.Remove(failedID);
                        }
                        else
                        {
                            deleteList.Add(failedID);
                        }
                    }
                    currentfailList.RemoveAll((ID => !deleteList.Contains(ID)));
                    maxReTry--;
                }
                prevfailList = new List<string>(currentfailList);
            }
            slaveCompletedList.AddRange(UpdateSlaveCompletedList(stepExecution, slaveStartedIDs));
            prevfailList.RemoveAll((item => slaveCompletedList.Contains(item)));
            deleteList = deleteList.Distinct<string>().ToList<string>();
            slaveStartedIDs.RemoveAll((item => deleteList.Contains(item)));
            return Tuple.Create<List<string>, List<string>>(slaveStartedIDs, prevfailList);
        }

        private List<string> UpdateSlaveCompletedList(StepExecution stepExecution, List<string> slaveStartedIDs)
        {
            List<string> slaveCompleteIDs = new List<string>();
            foreach (string slaveStartedId in slaveStartedIDs)
            {
                string targetmessage = slaveStartedId + ".COMPLETED";
                if (stepExecution.remoteChunking._slaveCompletedQueue.CheckMessageExistAndConsume(targetmessage))
                {
                    slaveCompleteIDs.Add(slaveStartedId);
                }
                   
            }
            return slaveCompleteIDs;
        }


        private bool WaitForAtLeastSlaveStarted(StepExecution stepExecution, int maxReTry, TimeSpan waitTime)
        {
            List<string> slaveStartedIDs = stepExecution.remoteChunking._slaveStartedQueue.GetSlaveIDByMasterName(stepExecution.StepName);
            if (slaveStartedIDs.Count == 0)
            {
                while(maxReTry > 0)
                {
                    Logger.Info("Wait for slave {0} seconds.", waitTime.TotalSeconds);
                    Thread.Sleep(waitTime);
                    slaveStartedIDs = stepExecution.remoteChunking._slaveStartedQueue.GetSlaveIDByMasterName(stepExecution.StepName);
                    if (slaveStartedIDs.Count > 0)
                    {
                        Dictionary<string, bool> slaveCurrentMap = stepExecution.remoteChunking._slaveMap;
                        foreach (string key in slaveStartedIDs)
                        {
                            if (!slaveCurrentMap.ContainsKey(key))
                            {
                                slaveCurrentMap[key] = true;
                            }
                        }
                        stepExecution.remoteChunking._slaveMap = slaveCurrentMap;
                        return true;
                    }
                    maxReTry--;
                }
                return false;
            }
            Dictionary<string, bool> slaveMap = stepExecution.remoteChunking._slaveMap;
            foreach (string key in slaveStartedIDs)
            {
                if (!slaveMap.ContainsKey(key))
                {
                    slaveMap[key] = true;
                }
            }
            stepExecution.remoteChunking._slaveMap = slaveMap;
            return true;
        }

        public void Master(StepExecution stepExecution, AutoResetEvent threadWait)
        {
            stepExecution.remoteChunking.CleanAllQueue();

            // configuration information includes stepname , slavexml name, and slave max numbers;
            string message = stepExecution.StepName + semicolon + SlaveXml + semicolon + slaveMaxNumber.ToString();

            // master send configuration information in the control queue for slave job to execute
            stepExecution.remoteChunking._controlQueue.Send(message);

            Thread controlThread = stepExecution.remoteChunking.controlThread;
            if (WaitForAtLeastSlaveStarted(stepExecution, _maxMasterWaitSlaveStartedRetry, _MasterWaitSlaveStartedtimeout))
            {
                threadWait.Set();
                bool Isterminate = false;
                Timer timer = new Timer(MasterTimerMethod, stepExecution, 0, _masterTimerseconds);
                while (!Isterminate)
                {
                    ControlQueue masterQueue = stepExecution.remoteChunking._masterQueue;
                    if ("FAILED".Equals(stepExecution.ExitStatus.ExitCode))
                    {
                        Isterminate = true;
                        timer.Dispose();
                        threadWait.Set();
                    }
                    else if ("COMPLETED".Equals(stepExecution.ExitStatus.ExitCode))
                    {
                        string masterCompletedMessage = "master." + stepExecution.StepName + "." + stepExecution.ExitStatus.ExitCode;
                        if (!masterQueue.CheckMessageExist(masterCompletedMessage))
                        {
                            masterQueue.Send(masterCompletedMessage);
                        }
                        else
                        {
                            List<string> failList = WaitForSlaveCompleted(stepExecution, slaveMaxNumber, _maxMasterwaitSlaveCompletedRetry, _MasterwaitSlaveCompletedTime);
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
            else
            {
                controlThread.Interrupt();
            }
        }

        private void MasterTimerMethod(object stepExectuionObject)
        {
            StepExecution stepExecution = (StepExecution)stepExectuionObject;
            AbstractExecutionListener.Logger.Debug("Timer for 5 second --------------------------------------------" + DateTime.Now.ToString());
            string stepName = stepExecution.StepName;
            ControlQueue masterLifeLineQueue = stepExecution.remoteChunking._masterLifeLineQueue;
            string masterNoAliveMessage = stepName + dot + bool.FalseString;
            if ("FAILED".Equals(stepExecution.ExitStatus.ExitCode))
            {
                masterLifeLineQueue.Send(masterNoAliveMessage);
                return;
            }
            else
            {
                Dictionary<string, bool> slaveMap = stepExecution.remoteChunking._slaveMap;
                List<string> slaveIDList = stepExecution.remoteChunking._slaveStartedQueue.GetSlaveIDByMasterName(stepExecution.StepName);
                foreach (string slaveID in slaveIDList)
                {
                    if (!slaveMap.ContainsKey(slaveID))
                    {
                        slaveMap[slaveID] = true;
                    }
                }
                ControlQueue slaveLifeLineQueue = stepExecution.remoteChunking._slaveLifeLineQueue;
                List<string> slaveIDs = new List<string>(slaveMap.Keys);
                foreach (string ID in slaveIDs)
                {
                    string targetmessage = ID + dot + bool.TrueString;
                    if (slaveLifeLineQueue.CheckMessageExistAndConsume(targetmessage))
                    {
                        slaveMap[ID] = true;
                        AbstractExecutionListener.Logger.Debug("slavekey: " + ID + " -----------Alive------------------");
                    }
                    else
                    {
                        slaveMap[ID] = false;
                        AbstractExecutionListener.Logger.Debug("slavekey: " + ID + " -----------NoAlive------------------");
                    }
                }
            }
        }

        public void Slave(StepExecution stepExecution, AutoResetEvent threadWait)
        {
            string slaveStartedMessage = stepExecution.StepName + dot + stepExecution.remoteChunking.SlaveID.ToString();
            stepExecution.remoteChunking._slaveStartedQueue.Send(slaveStartedMessage);
            Thread controlThread = stepExecution.remoteChunking.controlThread;
            Timer timer = new Timer(SlaveTimerMethod, stepExecution, 0, _slaveTimerseconds);
            bool Isterminate = false;
            while (!Isterminate)
            {
                ControlQueue slaveCompletedQueue = stepExecution.remoteChunking._slaveCompletedQueue;
                if ("FAILED".Equals(stepExecution.ExitStatus.ExitCode))
                {
                    timer.Dispose();
                    Isterminate = true;
                }
                else if ("COMPLETED".Equals(stepExecution.ExitStatus.ExitCode))
                {
                    string slaveCompletedMessage = stepExecution.JobExecution.JobInstance.JobName + dot + stepExecution.remoteChunking.SlaveID + ".COMPLETED";
                    slaveCompletedQueue.Send(slaveCompletedMessage);
                    timer.Dispose();
                    threadWait.Set();
                    Isterminate = true;
                }
            }
        }

        private void SlaveTimerMethod(object stepExectuionObject)
        {
            StepExecution stepExecution = (StepExecution)stepExectuionObject;
            string slaveIdMessage = stepExecution.StepName + dot + stepExecution.remoteChunking.SlaveID.ToString();
            Logger.Info("Timer for 5 second --------------------" + slaveIdMessage + "------------------------");
            ControlQueue slaveLifeLineQueue = stepExecution.remoteChunking._slaveLifeLineQueue;
            string slaveAliveMessage = slaveIdMessage + dot + bool.TrueString;
            string slaveNoAliveMessage = slaveIdMessage + dot + bool.FalseString;
            if ("FAILED".Equals(stepExecution.ExitStatus.ExitCode))
            {
                slaveLifeLineQueue.Send(slaveAliveMessage);
                Logger.Debug(slaveIdMessage + "-----------NoAlive------------------");
                return;
            }
            Logger.Debug(slaveIdMessage + "-----------Alive------------------");
            slaveLifeLineQueue.Send(slaveAliveMessage);
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