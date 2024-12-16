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
using Newtonsoft.Json.Linq;

namespace DIH.Data.Raw.MessageHandlers
{
    public class ReceiveFullBatchAbortMessageHandler : ReceiveFullbatchFunctionBase<ReceiveFullBatchAbortMessageHandler>
    {
        public ReceiveFullBatchAbortMessageHandler(
            ILogger<ReceiveFullBatchAbortMessageHandler> logger,
            IConfiguration configuration,
            IStorageServiceDictionary storageServiceDictionary,
            ITableService tableService,
            IDatabaseService databaseService,
            IMessagingServiceDictionary messageServiceDictionary,
            IFunctionsSettingsService functionsSettingsService,
            DataObjectTypeLocker dataObjectTypeLocker) : base(logger, configuration, storageServiceDictionary, tableService, databaseService, messageServiceDictionary, functionsSettingsService, dataObjectTypeLocker)
        {
        }

        public override bool CanHandleQueue(string queueName) => queueName == ReceiveAbortQueueName;

        public override async Task Run(string queueName, string messageText)
        {
            await ExecuteFunctionAsync<IngestionFullBatchMessage>(messageText, ProcessReceiveAbortMessage, AllMessagesProcessed, false, ReceiveAbortQueueName);
        }

        private async Task ProcessReceiveAbortMessage(IngestionFullBatchMessage message)
        {
            await AbortBatch(message);
        }

        private async Task AllMessagesProcessed(IngestionFullBatchMessage message)
        {
            Logger.DihInformation($"All upserted objects for {message.DataObjectTypeName} {message.BatchId} has been marked as aborted.");
            await ForwardBatchInfoToQueue(message, ReceiveCleanupQueueName);
        }

        private async Task AbortBatch(IngestionFullBatchMessage message)
        {
            var queryFilter = $"{JsonPropertyNames.DIH_UpdatingBatchId} = '{message.BatchId}'";
            var abortedObjects = DatabaseService.GetByQueryAsync<JObject>(message.DataObjectTypeName, queryFilter);

            await using (var bulkTaskAwaiter = new BulkTaskAwaiter(awaitAtMaxTasks: FunctionsSettingsService.MaxParallelResourceIntensiveTasks))
            {
                await foreach (var abortedObject in abortedObjects)
                {
                    var currentHash = abortedObject[JsonPropertyNames.DIH_Hash]?.ToString();
                    if (currentHash != null && !currentHash.EndsWith(JsonPropertyValues.DIH_AbortedHashAppendMark))
                    {
                        abortedObject[JsonPropertyNames.DIH_Hash] = $"{currentHash}{JsonPropertyValues.DIH_AbortedHashAppendMark}";
                        if (abortedObject[JsonPropertyNames.DIH_Status] != null && abortedObject[JsonPropertyNames.DIH_Status]?.ToString() == JsonPropertyValues.DIH_Status_SoftDeleted)
                        {
                            abortedObject[JsonPropertyNames.DIH_Status] = JsonPropertyValues.DIH_Status_Active;
                        }
                        await bulkTaskAwaiter.Add(DatabaseService.UpsertAsync(message.DataObjectTypeName, abortedObject));
                    }
                }
            }
        }
    }
}

