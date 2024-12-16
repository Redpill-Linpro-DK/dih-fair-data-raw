using DIH.Common;
using DIH.Common.Services.Messaging.Base;
using DIH.Common.Services.Settings;
using DIH.Common.Services.Table;
using DIH.Common.Tasks;
using Microsoft.Extensions.Configuration;

namespace DIH.Data.Raw.MessageHandlers.Helpers
{
    public class BatchMessageExecutionCoordinator<T> where T : class
    {
        private DihLogger<T> Logger { get; set; }
        private ITableService TableService { get; set; }
        private IFunctionsSettingsService FunctionsSettingsService { get; set; }
        private string ImportLogsTableName { get; set; }


        public BatchMessageExecutionCoordinator(
            DihLogger<T> logger,
            IConfiguration configuration,
            ITableService tableService,
            IFunctionsSettingsService functionsSettingsService)
        {
            Logger = logger;
            TableService = tableService;
            FunctionsSettingsService = functionsSettingsService;
            ImportLogsTableName = configuration[ConfigKeys.Data_Raw_TableImportLogs] ?? throw new InvalidOperationException($"Missing config {ConfigKeys.Data_Raw_TableImportLogs}");
        }

        /// <summary>
        /// Will execute a task once per { datatype:batch_id, segment_num } and execute allDoneTask after all segments has run.
        /// </summary>
        /// <param name="message">The batch message for the task</param>
        /// <param name="primaryTask">Task to execute for the message</param>
        /// <param name="allDoneTask">Optional. Executed whan all segments has been fully completed</param>
        /// <returns></returns>
        public async Task<bool> RunOncePerBatchSegmentAsync<Tmessage>(Tmessage message, Func<Tmessage, Task> primaryTask, string queueName, Func<Tmessage, Task>? allDoneTask = null) where Tmessage : IngestionFullBatchMessage, new()
        {
            var importLogMessage = await TableService.TryGetObjectAsync<IngestionFullBatchMessage>(ImportLogsTableName, message.DihKey, queueName);
            if (importLogMessage == null)
            {
                importLogMessage = new IngestionFullBatchMessage(message.DataObjectTypeName, message.BatchId, message.BatchSegment, message.BatchSegmentsTotal);
                await TableService.UpsertObjectAsync(ImportLogsTableName, importLogMessage.DihKey, queueName, importLogMessage);
            }
            else if (importLogMessage.Completed)
            {
                Logger.DihError($"Already executed '{queueName}' for batch id {message.BatchId} - duplicate batch id not allowed.");
                return false;
            }

            Func<Tmessage, Task> allDoneWithLogging = async (msg) =>
            {
                if (allDoneTask != null)
                {
                    await allDoneTask(msg);
                }

                importLogMessage.Completed = true;
                importLogMessage.CompletedTime = DateTime.Now;
                await TableService.UpsertObjectAsync(ImportLogsTableName, importLogMessage.DihKey, queueName, importLogMessage);
            };

            return await RunImpl(message, primaryTask, allDoneWithLogging, queueName);
        }

        private async Task<bool> RunImpl<Tmessage>(Tmessage message, Func<Tmessage, Task> primaryTask, Func<Tmessage, Task> allDoneTask, string queueName) where Tmessage : IngestionFullBatchMessage, new()
        {
            ArgumentNullException.ThrowIfNull(message, nameof(message));
            ArgumentNullException.ThrowIfNull(primaryTask, nameof(primaryTask));

            if (message.Completed) return false;

            var queueSegmentIdentifier = $"{queueName}:{message.BatchSegment}";

            Logger.DihDebug($"Checking existing execution of {queueSegmentIdentifier} for {message.DihKey}");

            try
            {
                var existingMessage = await TableService.TryGetObjectAsync<IngestionFullBatchMessage>(ImportLogsTableName, message.DihKey, queueSegmentIdentifier);

                if (existingMessage == null || !existingMessage.Completed)
                {
                    Logger.DihInformation($"Executing {queueSegmentIdentifier} for {message.DihKey}.");

                    // Write to table: Process has stated
                    await TableService.UpsertObjectAsync(ImportLogsTableName, message.DihKey, queueSegmentIdentifier, message);

                    // Execute the primary task
                    await primaryTask(message);

                    // Write to table: Process has completed
                    message.Completed = true;
                    message.CompletedTime = DateTime.Now;
                    await TableService.UpsertObjectAsync(ImportLogsTableName, message.DihKey, queueSegmentIdentifier, message);

                    if (allDoneTask != null)
                    {
                        var doneWithAllSegments = message.BatchSegmentsTotal == 1;

                        if (!doneWithAllSegments)
                        {
                            // From import log, Get rowKeys for batch segments related to this batch
                            List<string> batchSegmentLogRowKeys = await TableService.GetRowKeysAsync(ImportLogsTableName, message.DihKey).Where(rk => rk.StartsWith($"{queueName}:")).ToListAsync();

                            // If import log has entries for all batch segments, verify they all completed
                            if (batchSegmentLogRowKeys.Count == message.BatchSegmentsTotal)
                            {
                                // Get all import log messages for this queue
                                var messageResults = new List<IngestionFullBatchMessage>();
                                await using (var bulkTaskAwaiter = new BulkTaskAwaiter(FunctionsSettingsService.MaxParallelTasks))
                                {
                                    foreach (string rowKey in batchSegmentLogRowKeys)
                                    {
                                        await bulkTaskAwaiter.Add(TableService.TryGetObjectAsync<IngestionFullBatchMessage>(ImportLogsTableName, message.DihKey, rowKey));
                                    }
                                    messageResults = await bulkTaskAwaiter.AwaitAll<IngestionFullBatchMessage>();
                                }

                                var completedTasks = messageResults.Where(m => m != null && m.Completed).ToList();
                                // Check if all are done and if we have the latest time stamp
                                doneWithAllSegments = completedTasks.Count == message.BatchSegmentsTotal && message.CompletedTime == completedTasks.Max(m => m.CompletedTime);
                            }
                        }
                        if (doneWithAllSegments)
                        {
                            // Execute the "all done" task
                            await allDoneTask(message);
                        }
                    }

                    return true;
                }
                else
                {
                    Logger.DihError($"Already executed {ImportLogsTableName}:{message.DihKey} '{queueSegmentIdentifier}' - skipped");
                    return false;
                }
            }
            catch (Exception)
            {
                try
                {
                    // If this is failing, it's bad, but try/catch this since the first exception is way more important.
                    await TableService.DeleteAsync(ImportLogsTableName, message.DihKey, queueSegmentIdentifier);
                }
                catch (Exception unlockEx)
                {
                    Logger.DihError(unlockEx);
                }

                throw;
            }
        }
    }
}
