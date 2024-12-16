using DIH.Common;
using DIH.Common.Services.Database;
using DIH.Common.Services.Messaging;
using DIH.Common.Services.Settings;
using DIH.Common.Services.Storage;
using DIH.Common.Services.Table;
using DIH.Data.Raw.MessageHandlers.Helpers;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace DIH.Data.Raw.MessageHandlers
{
    public class ReceiveFullBatchMessageHandler : ReceiveFullbatchFunctionBase<ReceiveFullBatchMessageHandler>
    {
        PayloadHelper PayloadHelper { get; set; }
        public ReceiveFullBatchMessageHandler(
            ILogger<ReceiveFullBatchMessageHandler> logger,
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

        public override bool CanHandleQueue(string queueName) => queueName == ReceiveFullbatchQueueName;

        public override async Task Run(string queueName, string messageText)
        {
            await ExecuteFunctionAsync<IngestionFullBatchFileMessage>(messageText, ProcessReceiveMessage, AllMessagesProcessed, true, ReceiveFullbatchQueueName);
        }

        private async Task ProcessReceiveMessage(IngestionFullBatchFileMessage message)
        {
            if (message.BatchSegment < 1 || message.BatchSegment > message.BatchSegmentsTotal)
            {
                Logger.DihError($"Batch {message.DataObjectTypeName} {message.BatchId} segment number {message.BatchSegment} is not within expected range (Between 1 and {message.BatchSegmentsTotal}). Message dropped.");
                return;
            }

            if (message.BatchSegment == 1)
            {
                Logger.DihMonitorSuccess(DihMonitorSystemType.DIH, DihMonitorSystemName.DihIngestionFullbatch, DihMonitorSystemType.DIH, DihMonitorSystemName.DihRawFullbatchProcessing, message: $"Starting {message.DataObjectTypeName} raw data import batch consisting of {message.BatchSegmentsTotal} segments, batch id {message.BatchId}", batchId: message.BatchId, dataObjectTypeName: message.DataObjectTypeName);
            }

            Logger.DihInformation("Import payload into database");
            await ImportToDatabase(message);

            await TableService.UpsertObjectAsync(BatchesHandledTableName, message.DihKey, message.BatchSegment.ToString(), message);
        }

        private async Task AllMessagesProcessed(IngestionFullBatchFileMessage message)
        {
            await ForwardBatchInfoToQueue(message, ReceivePurgePlanQueueName);
        }

        private async Task ImportToDatabase(IngestionFullBatchFileMessage message)
        {
            Logger.DihDebug($"Access payload stream as json data objects");
            var storageService = StorageServiceDictionary.Get(message.StorageLayer);
            var payloadStream = await storageService.DownloadAsync(message.StorageContainerName, message.StoragePath);

            Func<IEnumerable<DataObjectIdentifier>, Task> unchangedIdsHandler = async (unchangedIdList) =>
                await TableService.UpsertBulkAsync(UnchangedIdsTableName, message.DihKey, unchangedIdList.Select(u => u.ToKeyString()).ToAsyncEnumerable(), FunctionsSettingsService.MaxParallelTasks);

            await PayloadHelper.UpsertChangesFromStream(payloadStream, message, (changedIdList) => Task.CompletedTask, unchangedIdsHandler);
        }
    }
}

