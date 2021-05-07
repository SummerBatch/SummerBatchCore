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

        //master
        private int _maxMasterWaitSlaveRetry = 3;
        private TimeSpan _MasterWaitSlave = TimeSpan.FromSeconds(5);
        private const int _masterTimerseconds = 5000;
        private TimeSpan _afterStepThreadTimeout = TimeSpan.FromMinutes(15);

        //slave
        private const int _slaveTimerseconds = 5000;
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

                    // wait for Master method send back slaveStarted signal
                    if (threadWait.WaitOne(_MasterWaitSlave * _maxMasterWaitSlaveRetry))
                    {
                        Logger.Info("Slave is started.");
                    }
                    else
                    {
                        // clean all message queues when no slave job provided
                        Logger.Info("Clean message in the Queue.");
                        stepExecution.remoteChunking.CleanAllQueue();
                        throw new JobExecutionException("No slave job provided");
                    }
                }
                else // slave step create control thread 
                {
                    Thread thread = new Thread((ThreadStart)(() => Slave(stepExecution, threadWait)));
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
                // determine slave / master mode for step 
                if (stepExecution.remoteChunking != null)
                {
                    // master part 
                    if (stepExecution.remoteChunking._master)
                    {
                        // wait for Master method send back signal
                        if (!stepExecution.remoteChunking.threadWait.WaitOne(_afterStepThreadTimeout))
                        {
                            throw new JobExecutionException("Master step failed");
                        }
                        // some slave failed master need to fail 
                        if ("FAILED".Equals(stepExecution.ExitStatus.ExitCode))
                        {
                            throw new JobExecutionException("Master step failed");
                        }

                        // clean all message queues when master completed 
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
            // batch failed need to join the thread
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

            List<string> slaveFailList = new List<string>();
            List<string> slaveCompletedList = new List<string>();

            // check slave existed and complete all of them 
            if (slaveIDcount > 0 && slaveIDcount <= slaveMaxNumber)
            {
                while (slaveIDcount > 0)
                {
                    // wait for slave to complete 
                    Thread.Sleep(waitTime);

                    // update completed slave
                    Dictionary<string, bool> slaveMap = stepExecution.remoteChunking._slaveMap;
                    List<string> slaveIDCurrentList = new List<string>(slaveMap.Keys);
                    slaveIDList.AddRange(new List<string>(slaveIDCurrentList));
                    slaveIDList = slaveIDList.Distinct<string>().ToList<string>();
                    slaveCompletedList.AddRange(UpdateSlaveCompletedList(stepExecution, slaveIDList));

                    // update faillist and completed slave regularly
                    Tuple<List<string>, List<string>> tuple = UpdateSlaveIDList(slaveMap, stepExecution, slaveIDList, slaveFailList, slaveCompletedList, maxReTry, waitTime);
                    int count = tuple.Item1.Count;
                    slaveFailList = tuple.Item2;

                    // all slaves are finished
                    if (count == 0)
                    {
                        Logger.Info("No slave need to wait .");
                        break;
                    }
                    Logger.Info("{0} slave need to complete", count);
                }
            }

            // clean message queue when all salves are completed
            Logger.Info("Clean message in the Queue.");
            stepExecution.remoteChunking.CleanAllQueue();
            return slaveFailList;
        }

        /// <summary>
        /// Update SlaveCompletedList and SlaveFailList.
        /// </summary>
        /// <param name="slaveMap"></param>
        /// <param name="stepExecution"></param>
        /// <param name="slaveStartedIDs"></param>
        /// <param name="prevfailList"></param>
        /// <param name="slaveCompletedList"></param>
        /// <param name="maxReTry"></param>
        /// <param name="waitRetryTime"></param>
        /// <returns></returns>
        private Tuple<List<string>, List<string>> UpdateSlaveIDList(Dictionary<string, bool> slaveMap, StepExecution stepExecution, List<string> slaveStartedIDs, List<string> slaveFailList, List<string> slaveCompletedList, int maxReTry, TimeSpan waitRetryTime)
        {
            List<string> deleteList = new List<string>();
            List<string> currentSlaveFailList = new List<string>();

            // get currentSlaveFailList from slaveMap in stepExecution
            foreach (string slaveStartedId in slaveStartedIDs)
            {
                if (!slaveMap[slaveStartedId])
                {
                    if (!slaveCompletedList.Contains(slaveStartedId))
                    {
                        currentSlaveFailList.Add(slaveStartedId);
                    }
                    deleteList.Add(slaveStartedId);
                }
            }

            // compare with previousSlavefailList
            if (currentSlaveFailList.Count != slaveFailList.Count)
            {
                // check slave is alive or not
                while (maxReTry > 0)
                {
                    Thread.Sleep(waitRetryTime);
                    Logger.Info("Retry to check slave is alive");
                    Dictionary<string, bool> slaveCurrentMap = stepExecution.remoteChunking._slaveMap;
                    foreach (string failedID in currentSlaveFailList)
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
                    currentSlaveFailList.RemoveAll((ID => !deleteList.Contains(ID)));
                    maxReTry--;
                }

                // update SlaveFailList with currentSlaveFailList
                slaveFailList = new List<string>(currentSlaveFailList);
            }

            // update SlaveFailList with slaveCompletedList
            slaveCompletedList.AddRange(UpdateSlaveCompletedList(stepExecution, slaveStartedIDs));
            slaveFailList.RemoveAll((item => slaveCompletedList.Contains(item)));

            // remove duplicate slaveID in the list
            deleteList = deleteList.Distinct<string>().ToList<string>();
            slaveStartedIDs.RemoveAll((item => deleteList.Contains(item)));
            return Tuple.Create<List<string>, List<string>>(slaveStartedIDs, slaveFailList);
        }

        /// <summary>
        /// Update SlaveCompletedList.
        /// </summary>
        /// <param name="stepExecution"></param>
        /// <param name="slaveStartedIDs"></param>
        /// <returns></returns>
        private List<string> UpdateSlaveCompletedList(StepExecution stepExecution, List<string> slaveStartedIDs)
        {
            List<string> slaveCompleteIDs = new List<string>();
            foreach (string slaveStartedId in slaveStartedIDs)
            {
                string targetmessage = slaveStartedId + dot + "COMPLETED";
                if (stepExecution.remoteChunking._slaveCompletedQueue.CheckMessageExistAndConsume(targetmessage))
                {
                    slaveCompleteIDs.Add(slaveStartedId);
                }
                   
            }
            return slaveCompleteIDs;
        }

        /// <summary>
        /// Wait at least one slave started.
        /// </summary>
        /// <param name="stepExecution"></param>
        /// <param name="maxReTry"></param>
        /// <param name="waitTime"></param>
        /// <returns></returns>
        private bool WaitForAtLeastSlaveStarted(StepExecution stepExecution, int maxReTry, TimeSpan waitTime)
        {
            List<string> slaveStartedIDs = stepExecution.remoteChunking._slaveStartedQueue.GetSlaveIDByMasterName(stepExecution.StepName);
            if (slaveStartedIDs.Count == 0)
            {
                // check at least one slave started
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

            // update slaveMap in the stepExecution
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

        /// <summary>
        /// Launch Master method in the controlThread.
        /// </summary>
        /// <param name="stepExecution"></param>
        /// <param name="threadWait"></param>
        public void Master(StepExecution stepExecution, AutoResetEvent threadWait)
        {
            // clean up all message queues
            stepExecution.remoteChunking.CleanAllQueue();

            // get slavexml name
            string SlaveXml = stepExecution.remoteChunking.SlaveFileName;

            // get slave max numbers
            int slaveMaxNumber = stepExecution.remoteChunking.SlaveMaxNumber;

            // configuration information includes stepname , slavexml name, and slave max numbers
            string message = stepExecution.StepName + semicolon + SlaveXml + semicolon + slaveMaxNumber.ToString();

            // master send configuration information in the control queue for slave job to execute
            stepExecution.remoteChunking._controlQueue.Send(message);

            // check at least one slave started
            if (WaitForAtLeastSlaveStarted(stepExecution, _maxMasterWaitSlaveRetry, _MasterWaitSlave))
            {
                // send back signal to the beforeStep
                threadWait.Set();
                bool Isterminate = false;

                // start a timer to check slave is still alive or not in every defualt seconds
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
                    // when batch completed send back signal to the afterStep and stop timer after send completedmessage to message queue and check all slaves completed
                    else if ("COMPLETED".Equals(stepExecution.ExitStatus.ExitCode))
                    {
                        string masterCompletedMessage = "master"+ dot + stepExecution.StepName + dot + stepExecution.ExitStatus.ExitCode;
                        if (!masterQueue.CheckMessageExist(masterCompletedMessage))
                        {
                            masterQueue.Send(masterCompletedMessage);
                        }
                        else
                        {
                            List<string> failList = WaitForSlaveCompleted(stepExecution, slaveMaxNumber, _maxMasterWaitSlaveRetry, _MasterWaitSlave);
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
            else // continue update slaveMap in the stepExecution and check slaves alive
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
                        Logger.Debug("slavekey: " + ID + " -----------Alive------------------");
                    }
                    else
                    {
                        slaveMap[ID] = false;
                        Logger.Debug("slavekey: " + ID + " -----------NoAlive------------------");
                    }
                }
            }
        }

        /// <summary>
        /// Launch Slave method in the controlThread.
        /// </summary>
        /// <param name="stepExecution"></param>
        /// <param name="threadWait"></param>
        public void Slave(StepExecution stepExecution, AutoResetEvent threadWait)
        {

            // slaveStartedMessage includes stepname and slave id
            string slaveStartedMessage = stepExecution.StepName + dot + stepExecution.remoteChunking.SlaveID.ToString();

            // slave send slaveStartedMessage in the slaveStarted queue for master job to check
            stepExecution.remoteChunking._slaveStartedQueue.Send(slaveStartedMessage);

            // start a timer to let master check slave is still alive or not in every defualt seconds
            Timer timer = new Timer(SlaveTimerMethod, stepExecution, 0, _slaveTimerseconds);
            bool Isterminate = false;
            while (!Isterminate)
            {
                // send back signal to the afterStep and stop timer when batch failed
                ControlQueue slaveCompletedQueue = stepExecution.remoteChunking._slaveCompletedQueue;
                if ("FAILED".Equals(stepExecution.ExitStatus.ExitCode))
                {
                    timer.Dispose();
                    Isterminate = true;
                }
                // when batch completed send back signal to the afterStep and stop timer after send completedmessage to message queue 
                else if ("COMPLETED".Equals(stepExecution.ExitStatus.ExitCode))
                {
                    string slaveCompletedMessage = stepExecution.JobExecution.JobInstance.JobName + dot + stepExecution.remoteChunking.SlaveID + dot + "COMPLETED";
                    slaveCompletedQueue.Send(slaveCompletedMessage);
                    timer.Dispose();
                    threadWait.Set();
                    Isterminate = true;
                }
            }
        }

        /// <summary>
        /// Launch Slave timer method in the controlThread.
        /// </summary>
        /// <param name="stepExectuionObject"></param>
        private void SlaveTimerMethod(object stepExectuionObject)
        {
            StepExecution stepExecution = (StepExecution)stepExectuionObject;
            string slaveIdMessage = stepExecution.StepName + dot + stepExecution.remoteChunking.SlaveID.ToString();
            Logger.Info("-------------------------------------------- Slave Timer --------------------------------------------");
            ControlQueue slaveLifeLineQueue = stepExecution.remoteChunking._slaveLifeLineQueue;
            string slaveAliveMessage = slaveIdMessage + dot + bool.TrueString;
            string slaveNoAliveMessage = slaveIdMessage + dot + bool.FalseString;

            // stop timer when batch failed
            if ("FAILED".Equals(stepExecution.ExitStatus.ExitCode))
            {
                slaveLifeLineQueue.Send(slaveNoAliveMessage);
                Logger.Debug(slaveIdMessage + "-----------NoAlive------------------");
                return;
            }
            else // continue send slaveAliveMessage to the slaveLifeLineQueue 
            {
                Logger.Debug(slaveIdMessage + "-----------Alive------------------");
                slaveLifeLineQueue.Send(slaveAliveMessage);
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