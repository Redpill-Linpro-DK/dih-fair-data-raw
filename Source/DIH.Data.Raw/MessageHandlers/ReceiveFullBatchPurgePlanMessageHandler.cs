using DIH.Common;
using DIH.Common.Services.Database;
using DIH.Common.Services.Messaging;
using DIH.Common.Services.Messaging.Base;
using DIH.Common.Services.Settings;
using DIH.Common.Services.Storage;
using DIH.Common.Services.Table;
using DIH.Data.Raw.MessageHandlers.Helpers;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace DIH.Data.Raw.MessageHandlers
{
    public class ReceiveFullBatchPurgePlanMessageHandler : ReceiveFullbatchFunctionBase<ReceiveFullBatchPurgePlanMessageHandler>
    {
        private int MaxDeletePercent { get; set; }

        public ReceiveFullBatchPurgePlanMessageHandler(
            ILogger<ReceiveFullBatchPurgePlanMessageHandler> logger,
            IConfiguration configuration,
            IStorageServiceDictionary storageServiceDictionary,
            ITableService tableService,
            IDatabaseService databaseService,
            IMessagingServiceDictionary messageServiceDictionary,
            IFunctionsSettingsService functionsSettingsService,
            DataObjectTypeLocker dataObjectTypeLocker) : base(logger, configuration, storageServiceDictionary, tableService, databaseService, messageServiceDictionary, functionsSettingsService, dataObjectTypeLocker)
        {
            string maxDeletePercentStr = configuration[ConfigKeys.Data_Raw_MaxDeletePercent];
            MaxDeletePercent = string.IsNullOrEmpty(maxDeletePercentStr) ? 100 : int.Parse(maxDeletePercentStr);
        }

        public override bool CanHandleQueue(string queueName) => queueName == ReceivePurgePlanQueueName;

        public override async Task Run(string queueName, string messageText)
        {
            await ExecuteFunctionAsync<IngestionFullBatchMessage>(messageText, ProcessReceivePurgePlanMessage, AllMessagesProcessed, true, ReceivePurgePlanQueueName);
        }

        private async Task ProcessReceivePurgePlanMessage(IngestionFullBatchMessage message)
        {
            Logger.DihInformation($"{message.DataObjectTypeName}, batch id {message.BatchId}, {message.BatchSegment} of {message.BatchSegmentsTotal}");

            var notThisBatchQueryFilter = $"{JsonPropertyNames.DIH_UpdatingBatchId} <> '{message.BatchId}' " +
                                          $"AND {JsonPropertyNames.DIH_Status} = '{JsonPropertyValues.DIH_Status_Active}'";
            var dbNotUpsertedCount = await DatabaseService.GetCountAsync(message.DataObjectTypeName, notThisBatchQueryFilter);

            var unchangedCount = await TableService.GetRowKeysAsync(UnchangedIdsTableName, message.DihKey).CountAsync();
            var toDeleteCount = dbNotUpsertedCount - unchangedCount;
            var dbCount = await DatabaseService.GetCountAsync(message.DataObjectTypeName);

            var toDeletePercent = (decimal)toDeleteCount / dbCount * 100;
            var maxDeletePercent = GetMaxDeletePercent(message.DataObjectTypeName);
            if (toDeletePercent > maxDeletePercent)
            {
                var err = $"Delete sanity-check failed: Batch id {message.BatchId} ({message.DataObjectTypeName}) would delete " +
                          $"{toDeletePercent:0.0}% ({toDeleteCount} of {dbCount}) of objects - maximum is {maxDeletePercent}%. " +
                          $"Batch will be aborted. Any imported objects will be marked aborted. To allow a higher delete % put a higher number in config " +
                          $"({ConfigKeys.Data_Raw_MaxDeletePercent} or {ConfigKeys.Data_Raw_MaxDeletePercent_PREFIX}{message.DataObjectTypeName})";

                throw new AbortEntireBatchException(err);
            }

            var purgeTasksTotal = (int)Math.Ceiling(Math.Max((decimal)toDeleteCount / FunctionsSettingsService.MaxTasksPerMessage, dbNotUpsertedCount / (FunctionsSettingsService.MaxTasksPerMessage * 10)));

            if (purgeTasksTotal > 0)
            {
                Logger.DihInformation($"Found {toDeleteCount} database objects to delete. Creating {purgeTasksTotal} task messages tasked with purging max {FunctionsSettingsService.MaxTasksPerMessage} each.");
                for (int purgeTask = 1; purgeTask <= purgeTasksTotal; purgeTask++)
                {
                    var purgeExecuteMessage = new IngestionFullBatchMessage(
                        message.DataObjectTypeName,
                        message.BatchId,
                        purgeTask,
                        purgeTasksTotal
                        );

                    await DataRawMessageService.EnqueueMessageAsync(ReceivePurgeExecuteQueueName, purgeExecuteMessage);
                }
                Logger.DihDebug($"Enqueued {purgeTasksTotal} task messages");
            }
            else
            {
                Logger.DihInformation($"Found no database objects to potentially delete. Moving on to broadcast.");
                await ForwardBatchInfoToQueue(message, ReceiveBroadcastQueueName);
            }
        }

        private async Task AllMessagesProcessed(IngestionFullBatchMessage message)
        {
            await Task.CompletedTask;
        }

        private int GetMaxDeletePercent(string dataObjectTypeName)
        {
            string maxDeletePercentStr = Configuration[$"{ConfigKeys.Data_Raw_MaxDeletePercent_PREFIX}{dataObjectTypeName}"];
            return string.IsNullOrEmpty(maxDeletePercentStr) ? MaxDeletePercent : int.Parse(maxDeletePercentStr);
        }
    }
}

