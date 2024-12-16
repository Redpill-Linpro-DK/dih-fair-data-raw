using DIH.Common;
using DIH.Common.Services.Database;
using DIH.Common.Services.Messaging;
using DIH.Common.Services.Messaging.Base;
using DIH.Common.Services.Settings;
using DIH.Common.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace DIH.Data.Raw.MessageHandlers.Helpers
{
    public class BroadcastHelper
    {
        private DihLogger<BroadcastHelper> Logger { get; set; }
        private IFunctionsSettingsService FunctionsSettingsService { get; set; }
        private IMessagingService DataRawMessageService { get; set; }
        private string DataChangeBroadcastTopicName { get; set; }

        public BroadcastHelper(
            ILogger<BroadcastHelper> logger,
            IConfiguration configuration,
            IFunctionsSettingsService functionsSettingsService,
            IMessagingServiceDictionary messageServiceDictionary)
        {
            Logger = logger.AsDihLogger();
            FunctionsSettingsService = functionsSettingsService;
            DataRawMessageService = messageServiceDictionary.Get(Layer.DataRaw);
            DataChangeBroadcastTopicName = configuration[ConfigKeys.Data_Raw_Change_TopicName] ?? throw new InvalidOperationException($"Missing config {ConfigKeys.Data_Raw_Change_TopicName}");
        }

        public async Task<int> BroadcastChanges(IIngestionMessage message, IEnumerable<DataObjectIdentifier> changedDataObjectIdentifiers, DataChangeType dataChangeType)
        {
            int affected = 0;
            List<DataObjectIdentifier> identifierList = new();
            await using (var bulkAwaiter = new BulkTaskAwaiter(awaitAtMaxTasks: FunctionsSettingsService.MaxParallelTasks))
            {
                foreach (var dataObjectIdentifier in changedDataObjectIdentifiers)
                {
                    identifierList.Add(dataObjectIdentifier);

                    if (identifierList.Count >= FunctionsSettingsService.MaxTasksPerMessage)
                    {
                        var changeMessage = new DataChangeMessage(message.DataObjectTypeName, Layer.DataRaw, identifierList, dataChangeType, message.BatchId);
                        await bulkAwaiter.Add(DataRawMessageService.EnqueueMessageAsync(DataChangeBroadcastTopicName, changeMessage, label: message.DataObjectTypeName));
                        Logger.DihInformation($"Broadcast '{dataChangeType}' change of {identifierList.Count} {message.DataObjectTypeName} items from batch {message.BatchId} on topic {DataChangeBroadcastTopicName}...");
                        affected += identifierList.Count;
                        identifierList.Clear();
                    }
                }
                if (identifierList.Count > 0)
                {
                    var changeMessage = new DataChangeMessage(message.DataObjectTypeName, Layer.DataRaw, identifierList, dataChangeType, message.BatchId);
                    await bulkAwaiter.Add(DataRawMessageService.EnqueueMessageAsync(DataChangeBroadcastTopicName, changeMessage, label: message.DataObjectTypeName));
                    Logger.DihInformation($"Broadcast '{dataChangeType}' change of {identifierList.Count} {message.DataObjectTypeName} items from batch {message.BatchId} on topic {DataChangeBroadcastTopicName}...");
                    affected += identifierList.Count;
                }
            }

            return affected;
        }

    }
}
