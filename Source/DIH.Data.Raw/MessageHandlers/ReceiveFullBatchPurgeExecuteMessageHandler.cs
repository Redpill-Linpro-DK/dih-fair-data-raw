using DIH.Common;
using DIH.Common.Services.Database;
using DIH.Common.Services.Messaging;
using DIH.Common.Services.Messaging.Base;
using DIH.Common.Services.Settings;
using DIH.Common.Services.Storage;
using DIH.Common.Services.Table;
using DIH.Common.Tasks;
using DIH.Data.Raw.MessageHandlers.Helpers;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace DIH.Data.Raw.MessageHandlers
{
    public class ReceiveFullBatchPurgeExecuteMessageHandler : ReceiveFullbatchFunctionBase<ReceiveFullBatchPurgeExecuteMessageHandler>
    {
        PayloadHelper PayloadHelper { get; set; }

        public ReceiveFullBatchPurgeExecuteMessageHandler(
            ILogger<ReceiveFullBatchPurgeExecuteMessageHandler> logger,
            IConfiguration configuration,
            IStorageServiceDictionary storageServiceDictionary,
            ITableService tableService,
            IDatabaseService databaseService,
            IMessagingServiceDictionary messageServiceDictionary,
            IFunctionsSettingsService functionsSettingsService,
            DataObjectTypeLocker dataObjectTypeLocker,
            PayloadHelper payloadHelper) : base(logger, configuration, storageServiceDictionary, tableService, databaseService, messageServiceDictionary, functionsSettingsService, dataObjectTypeLocker)
        {
            PayloadHelper = payloadHelper;
        }

        public override bool CanHandleQueue(string queueName) => queueName == ReceivePurgeExecuteQueueName;

        public override async Task Run(string queueName, string messageText)
        {
            await ExecuteFunctionAsync<IngestionFullBatchMessage>(messageText, ProcessReceivePurgeExecuteMessage, AllMessagesProcessed, true, ReceivePurgeExecuteQueueName);
        }

        private async Task ProcessReceivePurgeExecuteMessage(IngestionFullBatchMessage message)
        {
            Logger.DihInformation($"Delete old data objects from database (old = not part of this batch). Task {message.BatchSegment} of {message.BatchSegmentsTotal}.");
            await DeleteStaleFromDatabase(message);
        }
        private async Task AllMessagesProcessed(IngestionFullBatchMessage message)
        {
            await ForwardBatchInfoToQueue(message, ReceiveBroadcastQueueName);
        }

        private async Task DeleteStaleFromDatabase(IngestionFullBatchMessage message)
        {
            // Delete data objects from database that was NOT part of this bulk
            var notThisBatchQueryFilter =
                $"{JsonPropertyNames.DIH_UpdatingBatchId} <> '{message.BatchId}' " +
                $"AND {JsonPropertyNames.DIH_Status} = '{JsonPropertyValues.DIH_Status_Active}' ";
            var potentiallyOldIdentifiers = DatabaseService.GetIdentifiersAsync(message.DataObjectTypeName, notThisBatchQueryFilter, zeroBasedSegment: message.BatchSegment - 1, segmentsTotal: message.BatchSegmentsTotal);

            var deleteCount = 0;

            await using (var bulkAwaiter = new BulkTaskAwaiter(awaitAtMaxTasks: FunctionsSettingsService.MaxParallelResourceIntensiveTasks))
            {
                await foreach (var potentiallyOldIdentifier in potentiallyOldIdentifiers)
                {
                    var potentialDeleteTask = new Func<Task>(async () =>
                    {
                        var isPartOfBatch = await TableService.ExistsAsync(UnchangedIdsTableName, message.DihKey, potentiallyOldIdentifier.ToKeyString());

                        if (!isPartOfBatch)
                        {
                            await PayloadHelper.DeleteDataObject(potentiallyOldIdentifier, message);
                            await TableService.UpsertAsync(DeletedIdsTableName, message.DihKey, potentiallyOldIdentifier.ToKeyString());
                        }
                    });

                    await bulkAwaiter.Add(potentialDeleteTask());
                    deleteCount += 1;
                }
            }

            Logger.DihInformation($"Data Raw PurgeExecute {message.BatchSegment}/{message.BatchSegmentsTotal} deleted {deleteCount} stale data objects from database.");
        }
    }
}

