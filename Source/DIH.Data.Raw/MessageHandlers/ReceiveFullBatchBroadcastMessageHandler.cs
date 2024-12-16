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
    public class ReceiveFullBatchBroadcastMessageHandler : ReceiveFullbatchFunctionBase<ReceiveFullBatchBroadcastMessageHandler>
    {
        private BroadcastHelper BroadcastHelper { get; set; }

        public ReceiveFullBatchBroadcastMessageHandler(
            ILogger<ReceiveFullBatchBroadcastMessageHandler> logger,
            IConfiguration configuration,
            IStorageServiceDictionary storageServiceDictionary,
            ITableService tableService,
            IDatabaseService databaseService,
            IMessagingServiceDictionary messageServiceDictionary,
            IFunctionsSettingsService functionsSettingsService,
            DataObjectTypeLocker dataObjectTypeLocker,
            BroadcastHelper broadcastHelper) : base(logger, configuration, storageServiceDictionary, tableService, databaseService, messageServiceDictionary, functionsSettingsService, dataObjectTypeLocker)
        {
            BroadcastHelper = broadcastHelper;
        }

        public override bool CanHandleQueue(string queueName) => queueName == ReceiveBroadcastQueueName;

        public override async Task Run(string queueName, string messageText)
        {
            await ExecuteFunctionAsync<IngestionFullBatchMessage>(messageText, ProcessReceiveBroadcastMessage, AllMessagesProcessed, true, ReceiveBroadcastQueueName);
        }

        private async Task ProcessReceiveBroadcastMessage(IngestionFullBatchMessage message)
        {
            Logger.DihInformation("Broadcast change messages ...");
            await BroadcastChanges(message);
        }
        private async Task AllMessagesProcessed(IngestionFullBatchMessage message)
        {
            await ForwardBatchInfoToQueue(message, ReceiveCleanupQueueName);
        }

        private async Task BroadcastChanges(IngestionFullBatchMessage message)
        {
            var thisBatchQueryFilter = $"{JsonPropertyNames.DIH_UpdatingBatchId} = '{message.BatchId}' " +
                                       $"AND {JsonPropertyNames.DIH_Status} = '{JsonPropertyValues.DIH_Status_Active}'";
            var upsertedIds = DatabaseService.GetIdentifiersAsync(message.DataObjectTypeName, thisBatchQueryFilter);
            int upsertedCount = await BroadcastHelper.BroadcastChanges(message, upsertedIds.ToEnumerable(), DataChangeType.AddOrUpdate);

            var deletedIds = TableService.GetRowKeysAsync(DeletedIdsTableName, message.DihKey);
            int deletedCount = await BroadcastHelper.BroadcastChanges(message, deletedIds.ToEnumerable().Select(keyString => DataObjectIdentifier.FromKeyString(keyString)), DataChangeType.Delete);

            Logger.DihMonitorSuccess(DihMonitorSystemType.DIH, DihMonitorSystemName.DihRawFullbatchProcessing, DihMonitorSystemType.DIH, DihMonitorSystemName.DihRawChange, message: $"Completed {message.DataObjectTypeName} raw data import batch '{message.BatchId}', {upsertedCount} objects upserted, {deletedCount} deleted", batchId: message.BatchId, dataObjectTypeName: message.DataObjectTypeName);
        }
    }
}

