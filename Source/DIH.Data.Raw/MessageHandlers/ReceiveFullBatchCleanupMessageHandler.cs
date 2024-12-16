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
    public class ReceiveFullBatchCleanupMessageHandler : ReceiveFullbatchFunctionBase<ReceiveFullBatchCleanupMessageHandler>
    {
        public ReceiveFullBatchCleanupMessageHandler(
            ILogger<ReceiveFullBatchCleanupMessageHandler> logger,
            IConfiguration configuration,
            IStorageServiceDictionary storageServiceDictionary,
            ITableService tableService,
            IDatabaseService databaseService,
            IMessagingServiceDictionary messageServiceDictionary,
            IFunctionsSettingsService functionsSettingsService,
            DataObjectTypeLocker dataObjectTypeLocker) : base(logger, configuration, storageServiceDictionary, tableService, databaseService, messageServiceDictionary, functionsSettingsService, dataObjectTypeLocker)
        {
        }

        public override bool CanHandleQueue(string queueName) => queueName == ReceiveCleanupQueueName;

        public override async Task Run(string queueName, string messageText)
        {
            await ExecuteFunctionAsync<IngestionFullBatchMessage>(messageText, ProcessReceiveCleanupMessage, AllMessagesProcessed, false, ReceiveCleanupQueueName);
        }

        private async Task ProcessReceiveCleanupMessage(IngestionFullBatchMessage message)
        {
            await CleanUp(message);

            Logger.DihInformation($"#   -------------------------------------------------------------------------------");
            Logger.DihInformation($"#");
            Logger.DihInformation($"#   {message.DataObjectTypeName} Batch with id {message.BatchId} COMPLETED!");
            Logger.DihInformation($"#");
            Logger.DihInformation($"#   -------------------------------------------------------------------------------");
        }

        private async Task AllMessagesProcessed(IngestionFullBatchMessage message)
        {
            await Task.CompletedTask;
        }

        private async Task CleanUp(IngestionFullBatchMessage message)
        {
            try
            {
                Logger.DihInformation("Clear table with IDs that was in batch, but was unchanged (not upserted)");
                var unchangedIds = TableService.GetRowKeysAsync(UnchangedIdsTableName, message.DihKey);
                await TableService.DeleteBulkAsync(UnchangedIdsTableName, message.DihKey, unchangedIds, maxParallelism: FunctionsSettingsService.MaxParallelTasks);

                Logger.DihInformation("Clear table with IDs we deleted");
                var deletedIds = TableService.GetRowKeysAsync(DeletedIdsTableName, message.DihKey);
                await TableService.DeleteBulkAsync(DeletedIdsTableName, message.DihKey, deletedIds, maxParallelism: FunctionsSettingsService.MaxParallelTasks);

                //Logger.DihInformation("Clear (move) files");
                //var batchFileMessages = TableService.GetObjectsAsync<IngestionFullBatchFileMessage>(BatchesHandledTableName, message.DihKey);

                //// Clear (move) blobs
                //await foreach (var batchFileMessage in batchFileMessages)
                //{
                //    await CleanUpStorage(batchFileMessage);
                //}
            }
            finally
            {
                Logger.DihInformation($"Unlock data type so future batches are allowed for {message.DataObjectTypeName}");
                await DataObjectTypeLocker.RemoveLock(message);
            }
        }

        //private async Task CleanUpStorage(IngestionFullBatchFileMessage batchFileMessage)
        //{
        //    // Clean up json file on storage
        //    var filename = batchFileMessage.StoragePath.Split('/').Last();
        //    var newPath = $"{batchFileMessage.StoragePath.Replace(filename, "")}RawDataImported/{DateTime.Now.ToString("yyyy/MM/dd")}/{filename}";
        //    var storageService = StorageServiceDictionary.Get(batchFileMessage.StorageLayer);

        //    try
        //    {
        //        await storageService.MoveAsync(batchFileMessage.StorageContainerName, batchFileMessage.StoragePath, newPath, deleteOld: true);

        //        // Store new path for later deletion (timer)
        //        batchFileMessage.StoragePath = newPath;
        //        await TableService.UpsertObjectAsync(BatchesHandledTableName, batchFileMessage.DihKey, batchFileMessage.BatchSegment.ToString(), batchFileMessage);

        //        Logger.DihDebug($"Moved blob {filename} to {newPath.Replace(filename, "")}");
        //    }
        //    catch (Exception)
        //    {
        //        Logger.DihWarning("Could not clean up blob, probably already moved.");
        //    }
        //}

    }
}

