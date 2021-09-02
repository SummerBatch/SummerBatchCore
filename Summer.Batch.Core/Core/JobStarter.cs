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
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using NLog;
using Summer.Batch.Core.Core.Unity.Xml;
using Summer.Batch.Core.Explore;
using Summer.Batch.Core.Launch;
using Summer.Batch.Core.Launch.Support;
using Summer.Batch.Core.Unity;
using Summer.Batch.Core.Unity.Xml;
using Summer.Batch.Data;

namespace Summer.Batch.Core
{
    /// <summary>
    /// Job starter. Used to start or re-start jobs
    /// </summary>
    public static class JobStarter
    {
        private static readonly Logger Logger = LogManager.GetCurrentClassLogger();
        private const string controlQueueName = "control";
        /// <summary>
        /// Starts given job.
        /// </summary>
        /// <param name="xmlJobFile"></param>
        /// <param name="loader"></param>
        /// <returns></returns>
        public static JobExecution Start(string xmlJobFile, UnityLoader loader)
        {
            var job = XmlJobParser.LoadJob(xmlJobFile);
            loader.Job = job;
            var jobOperator = (SimpleJobOperator)BatchRuntime.GetJobOperator(loader);
            var executionId = jobOperator.StartNextInstance(job.Id);
            return jobOperator.JobExplorer.GetJobExecution((long)executionId);
        }

        /// <summary>
        /// Restarts given job.
        /// </summary>
        /// <param name="xmlJobFile"></param>
        /// <param name="loader"></param>
        /// <returns></returns>
        public static JobExecution ReStart(string xmlJobFile, UnityLoader loader)
        {
            var job = XmlJobParser.LoadJob(xmlJobFile);
            loader.Job = job;
            var jobOperator = (SimpleJobOperator)BatchRuntime.GetJobOperator(loader);
            var jobExecution = GetLastFailedJobExecution(job.Id, jobOperator.JobExplorer);
            if (jobExecution == null)
            {
                throw new JobExecutionNotFailedException(
                    String.Format("No failed or stopped execution found for job={0}" , job.Id));
            }
            var executionId = jobOperator.Restart(jobExecution.Id.Value);
            return jobOperator.JobExplorer.GetJobExecution((long)executionId);
        }

        /// <summary>
        /// Stops a given running job.
        /// </summary>
        /// <param name="xmlJobFile"></param>
        /// <param name="loader"></param>
        public static void Stop(string xmlJobFile, UnityLoader loader)
        {
            var job = XmlJobParser.LoadJob(xmlJobFile);
            loader.Job = job;
            var jobOperator = (SimpleJobOperator)BatchRuntime.GetJobOperator(loader);
            var jobExecutions = GetRunningJobExecutions(job.Id, jobOperator.JobExplorer);
            if (jobExecutions == null || !jobExecutions.Any())
            {
                throw new JobExecutionNotFailedException(
                    string.Format("No running execution found for job={0}", job.Id));
            }
            foreach (var jobExecution in jobExecutions)
            {
                jobExecution.Status = BatchStatus.Stopping;
                jobOperator.JobRepository.Update(jobExecution);
            }
            Logger.Info("Job {0} was stopped.", job.Id);
        }

        /// <summary>
        /// Abandons a given running job.
        /// </summary>
        /// <param name="xmlJobFile"></param>
        /// <param name="loader"></param>
        public static void Abandon(string xmlJobFile, UnityLoader loader)
        {
            var job = XmlJobParser.LoadJob(xmlJobFile);
            loader.Job = job;
            var jobOperator = (SimpleJobOperator)BatchRuntime.GetJobOperator(loader);
            var jobExecutions = GetStoppedJobExecutions(job.Id, jobOperator.JobExplorer);
            if (jobExecutions == null || !jobExecutions.Any())
            {
                throw new JobExecutionNotFailedException(
                    String.Format("No stopped execution found for job={0}", job.Id));
            }
            foreach (var jobExecution in jobExecutions)
            {
                jobExecution.Status = BatchStatus.Abandoned;
                jobOperator.JobRepository.Update(jobExecution);
            }
            Logger.Info("Job {0} was abandoned.", job.Id);
        }

        /// <summary>
        /// Retrieves the last failed job execution for the given job identifier;
        /// Might be null if not found.
        /// </summary>
        /// <param name="jobIdentifier"></param>
        /// <param name="jobExplorer"></param>
        /// <returns></returns>
        private static JobExecution GetLastFailedJobExecution(string jobIdentifier, IJobExplorer jobExplorer)
        {
            List<JobExecution> jobExecutions =
                GetJobExecutionsWithStatusGreaterThan(jobIdentifier, BatchStatus.Stopping, jobExplorer);
            if (!jobExecutions.Any())
            {
                return null;
            }
            return jobExecutions[0];
        }

        /// <summary>
        /// Retrieves the running job executions for the given job identifier;
        /// Might be null if nothing found.
        /// </summary>
        /// <param name="jobIdentifier"></param>
        /// <param name="jobExplorer"></param>
        /// <returns></returns>
        private static List<JobExecution> GetRunningJobExecutions(string jobIdentifier, IJobExplorer jobExplorer)
        {
            List<JobExecution> jobExecutions =
                GetJobExecutionsWithStatusGreaterThan(jobIdentifier, BatchStatus.Completed, jobExplorer);
            if (!jobExecutions.Any())
            {
                return null;
            }
            return jobExecutions.Where(jobExecution => jobExecution.IsRunning()).ToList();
        }

        /// <summary>
        /// Retrieves the stopped job executions for the given job identifier;
        /// Might be null if nothing found.
        /// </summary>
        /// <param name="jobIdentifier"></param>
        /// <param name="jobExplorer"></param>
        /// <returns></returns>
        private static List<JobExecution> GetStoppedJobExecutions(string jobIdentifier, IJobExplorer jobExplorer)
        {
            List<JobExecution> jobExecutions =
                GetJobExecutionsWithStatusGreaterThan(jobIdentifier, BatchStatus.Started, jobExplorer);
            if (!jobExecutions.Any())
            {
                return null;
            }
            return jobExecutions.Where(jobExecution => jobExecution.Status != BatchStatus.Abandoned).ToList();
        }

        /// <summary>
        /// Returns the given jobidentifier as long or null if conversion cannot be completed.
        /// </summary>
        /// <param name="jobIdentifier"></param>
        /// <returns></returns>
        private static long? GetLongIdentifier(string jobIdentifier)
        {
            try
            {
                return Convert.ToInt64(jobIdentifier);
            }
            catch (Exception)
            {
                // Not an ID - must be a name
                return null;
            }
        }

        /// <summary>
        /// Retrieves all job executions for a given minimal status 
        /// </summary>
        /// <param name="jobIdentifier"></param>
        /// <param name="minStatus"></param>
        /// <param name="jobExplorer"></param>
        /// <returns></returns>
        private static List<JobExecution> GetJobExecutionsWithStatusGreaterThan(string jobIdentifier, BatchStatus minStatus, IJobExplorer jobExplorer)
        {

            long? executionId = GetLongIdentifier(jobIdentifier);
            if (executionId != null)
            {
                JobExecution jobExecution = jobExplorer.GetJobExecution(executionId.Value);
                if (jobExecution.Status.IsGreaterThan(minStatus))
                {
                    return new List<JobExecution> { jobExecution };
                }
                //empmty list
                return new List<JobExecution>();
            }

            int start = 0;
            int count = 100;
            List<JobExecution> executions = new List<JobExecution>();
            IList<JobInstance> lastInstances = jobExplorer.GetJobInstances(jobIdentifier, start, count);

            while (lastInstances.Any())
            {

                foreach (JobInstance jobInstance in lastInstances)
                {
                    IList<JobExecution> jobExecutions = jobExplorer.GetJobExecutions(jobInstance);
                    if (jobExecutions == null || !jobExecutions.Any())
                    {
                        continue;
                    }
                    executions.AddRange(jobExecutions.Where(jobExecution => jobExecution.Status.IsGreaterThan(minStatus)));
                }

                start += count;
                lastInstances = jobExplorer.GetJobInstances(jobIdentifier, start, count);

            }

            return executions;

        }

        /// <summary>
        /// Enum of possible results.
        /// </summary>
        public enum Result
        {
            /// <summary>
            /// Success enum litteral
            /// </summary>
            Success = 0,
            /// <summary>
            /// Failed enum litteral
            /// </summary>
            Failed = 1,
            /// <summary>
            /// InvalidOption enum litteral
            /// </summary>
            InvalidOption = 2
        }
        public static JobExecution WorkerStart(string xmlJobFile, string hostName, UnityLoader loader, int workerUpdateTimeInterval = 15)
        {

            ControlQueue _controlQueue = GetControlQueue(controlQueueName, hostName);


            int messageCount = _controlQueue.GetMessageCount();
            XmlJob job = null;
            TimeSpan WorkerUpdatetimeInterval = TimeSpan.FromSeconds(workerUpdateTimeInterval);
            if(messageCount != 0)
            {
                for (int i = 0; i < messageCount; i++)
                {
                    string fileName = Path.GetFileName(xmlJobFile);
                    string message = _controlQueue.Receive(fileName);
                    if (ValidateMessage(message)) // for 3 items
                    {
                        Tuple<XmlJob, string, int, bool> tuple = ValidateFileName(message, xmlJobFile);
                        if (tuple.Item4)
                        {
                            // Resend the message to the controlQueue
                            if (tuple.Item3 > 0)
                            {
                                _controlQueue.Send(tuple.Item2);
                            }
                            job = tuple.Item1;
                            Guid guid = Guid.NewGuid();
                            foreach (XmlStep step in job.JobElements)
                            {
                                step.RemoteChunking = new XmlRemoteChunking();
                                step.RemoteChunking.HostName = hostName;
                                step.RemoteChunking.Master = false;
                                step.RemoteChunking.WorkerID = guid.ToString();
                            }
                            break;
                        }
                    }
                }
            }
            else
            {
                do
                {
                    messageCount = _controlQueue.GetMessageCount();
                    if (messageCount != 0)
                    {
                        for (int i = 0; i < messageCount; i++)
                        {
                            string fileName = Path.GetFileName(xmlJobFile);
                            string message = _controlQueue.Receive(fileName);
                            if (ValidateMessage(message)) // for 3 items
                            {
                                Tuple<XmlJob, string, int, bool> tuple = ValidateFileName(message, xmlJobFile);
                                if (tuple.Item4)
                                {
                                    // Resend the message to the controlQueue
                                    if (tuple.Item3 > 0)
                                    {
                                        _controlQueue.Send(tuple.Item2);
                                    }
                                    job = tuple.Item1;
                                    Guid guid = Guid.NewGuid();
                                    foreach (XmlStep step in job.JobElements)
                                    {
                                        step.RemoteChunking = new XmlRemoteChunking();
                                        step.RemoteChunking.HostName = hostName;
                                        step.RemoteChunking.Master = false;
                                        step.RemoteChunking.WorkerID = guid.ToString();
                                    }
                                    break;
                                }
                            }
                        }
                        break;
                    }
                    else
                    {
                        Logger.Info("No master job provided. Wait for worker {0} seconds.", WorkerUpdatetimeInterval.TotalSeconds);
                        Thread.Sleep(WorkerUpdatetimeInterval);
                        //throw new JobExecutionException("No master job provided");
                    }
                } while (messageCount == 0);
            }
           
            
            _controlQueue.Requeue();
            loader.Job = job;
            var jobOperator = (SimpleJobOperator)BatchRuntime.GetJobOperator(loader);
            var executionId = jobOperator.StartNextInstance(job.Id);

            return jobOperator.JobExplorer.GetJobExecution((long)executionId);
        }

        private static ControlQueue GetControlQueue(string queuename, string hostname)
        {
            try
            {
                QueueConnectionProvider queueConnectionProvider = new QueueConnectionProvider();
                queueConnectionProvider.HostName = hostname;
                ControlQueue _controlQueue = new ControlQueue();
                _controlQueue.ConnectionProvider = queueConnectionProvider;
                _controlQueue.QueueName = queuename;
                _controlQueue.CreateQueue();
                return _controlQueue;
            }
            catch (Exception e)
            {
                throw e.InnerException;
            }
        }

        private static bool ValidateMessage(string message)
        {
            if (!string.IsNullOrWhiteSpace(message) && message.Split(';').Length == 3)
            {
                return true;
            }
            return false;
        }

        private static Tuple<XmlJob, string, int, bool> ValidateFileName(string message, string xmlJobFile)
        {
            var tuple = new Tuple<XmlJob, string, int, bool>(null, "", 0, false);
            string[] splitMessage = message.Split(';');
            string stepName = splitMessage[0];
            string xmlFileName = splitMessage[1];
            int workerNumber = (Int32.TryParse(splitMessage[2], out int value)) ? value : 0;

            if (Path.GetFileName(xmlJobFile).Equals(xmlFileName))
            {

                XmlJob job = XmlJobParser.LoadJob(xmlJobFile);
                var step = job.JobElements.Find(x => x.Id == stepName);

                if (step != null && workerNumber > 0)
                {
                    StringBuilder stringBuilder = new StringBuilder();
                    workerNumber--;
                    stringBuilder.Append(stepName + ";" + xmlFileName + ";" + (workerNumber).ToString());
                    return new Tuple<XmlJob, string, int, bool>(job, stringBuilder.ToString(), workerNumber, true);
                }
                return tuple;
            }


            return tuple;
        }
    }
}
