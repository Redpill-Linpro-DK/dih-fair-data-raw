using DIH.Common;
using DIH.Common.Services.Database;
using DIH.Common.Services.Messaging;
using DIH.Common.Services.Settings;
using DIH.Common.Services.Storage;
using DIH.Common.Services.Table;
using DIH.Common.Tasks;
using DIH.Data.Raw.MessageHandlers.Helpers;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace DIH.Data.Raw.MessageHandlers
{
    public class ReceiveChangeMessageHandler : IMessageHandler
    {
        private DihLogger<ReceiveChangeMessageHandler> Logger { get; set; }
        private IConfiguration Configuration { get; set; }
        private IStorageServiceDictionary StorageServiceDictionary { get; set; }
        private IDatabaseService DatabaseService { get; set; }
        private IFunctionsSettingsService FunctionsSettingsService { get; set; }
        private ITableService TableService { get; set; }
        private PayloadHelper PayloadHelper { get; set; }
        private BroadcastHelper BroadcastHelper { get; set; }

        // Queue names
        private string ReceiveChangeRawDataSubscriptionName { get; set; }

        // Table names
        private string ChangesHandledTableName { get; set; }

        public ReceiveChangeMessageHandler(
            ILogger<ReceiveChangeMessageHandler> logger,
            IConfiguration configuration,
            IStorageServiceDictionary storageServiceDictionary,
            IDatabaseService databaseService,
            IFunctionsSettingsService functionsSettingsService,
            ITableService tableService,
            PayloadHelper payloadHelper,
            BroadcastHelper broadcastHelper)
        {
            Logger = logger.AsDihLogger();
            Configuration = configuration;
            StorageServiceDictionary = storageServiceDictionary;
            DatabaseService = databaseService;
            FunctionsSettingsService = functionsSettingsService;
            TableService = tableService;
            PayloadHelper = payloadHelper;
            BroadcastHelper = broadcastHelper;

            // Queue names
            ReceiveChangeRawDataSubscriptionName = configuration[ConfigKeys.Data_Raw_ReceiveChange_SubscriptionName] ?? throw new InvalidOperationException($"Missing config {ConfigKeys.Data_Raw_ReceiveChange_SubscriptionName}");

            // Table names
            ChangesHandledTableName = configuration[ConfigKeys.Data_Raw_TableChangesHandled] ?? throw new InvalidOperationException($"Missing config {ConfigKeys.Data_Raw_TableChangesHandled}");
        }

        public bool CanHandleQueue(string queueName) => ReceiveChangeRawDataSubscriptionName == queueName;

        public async Task Run(string queueName, string queueMessage)
        {
            string? batchId = null;
            string? dataObjectTypeName = null;
            try
            {
                var message = DeserializeMessage<IngestionChangeMessage>(queueMessage);
                if (message == null)
                {
                    throw new InvalidOperationException($"Unable to deserialize the message.");
                }

                Logger.DihDebug($"Processing {ReceiveChangeRawDataSubscriptionName} message {message.DihKey}");
                batchId = message.BatchId;
                dataObjectTypeName = message.DataObjectTypeName;
                var payloadStream = await message.GetPayloadStream(StorageServiceDictionary);

                switch (message.ChangeType)
                {
                    case DataChangeType.AddOrUpdate:
                        Func<IEnumerable<DataObjectIdentifier>, Task> changedIdsHandler = async (changedIdList) =>
                            await BroadcastHelper.BroadcastChanges(message, changedIdList, DataChangeType.AddOrUpdate);
                        int affectedObjects = await PayloadHelper.UpsertChangesFromStream(payloadStream, message, changedIdsHandler, (unchangedIdList) => Task.CompletedTask);
                        Logger.DihMonitorSuccess(DihMonitorSystemType.DIH, DihMonitorSystemName.DihIngestionChange, DihMonitorSystemType.DIH, DihMonitorSystemName.DihRawChange, message: $"Upserted {affectedObjects} raw objects of type {dataObjectTypeName}", batchId: batchId, dataObjectTypeName: dataObjectTypeName);
                        break;
                    case DataChangeType.Delete:
                        var dataObjects = PayloadHelper.GetRawObjectsFromStream(payloadStream, message);
                        var deletedIds = new List<DataObjectIdentifier>();

                        await using (var bulkAwaiter = new BulkTaskAwaiter(awaitAtMaxTasks: FunctionsSettingsService.MaxParallelTasks))
                        {
                            await foreach (var dataObject in dataObjects)
                            {
                                var dataObjectId = DatabaseService.GetDataObjectIdentifier(message.DataObjectTypeName, dataObject);
                                await bulkAwaiter.Add(PayloadHelper.DeleteDataObject(dataObjectId, message));
                                deletedIds.Add(dataObjectId);
                            }
                        }

                        await BroadcastHelper.BroadcastChanges(message, deletedIds, DataChangeType.Delete);
                        Logger.DihMonitorSuccess(DihMonitorSystemType.DIH, DihMonitorSystemName.DihIngestionChange, DihMonitorSystemType.DIH, DihMonitorSystemName.DihRawChange, message: $"Deleted {deletedIds.Count} raw objects of type {dataObjectTypeName}", batchId: batchId, dataObjectTypeName: dataObjectTypeName);
                        break;
                    case DataChangeType.Patch:
                        var patchObjects = PayloadHelper.GetRawObjectsFromStream(payloadStream, message);
                        Func<IEnumerable<DataObjectIdentifier>, Task> patchedIdsHandler = async (patchedIdList) =>
                            await BroadcastHelper.BroadcastChanges(message, patchedIdList, DataChangeType.AddOrUpdate);

                        await PayloadHelper.PatchChangesFromStream(message, patchObjects, patchedIdsHandler);
                        Logger.DihMonitorSuccess(DihMonitorSystemType.DIH, DihMonitorSystemName.DihIngestionChange, DihMonitorSystemType.DIH, DihMonitorSystemName.DihRawChange, message: $"Completed patch of objects of type {dataObjectTypeName}", batchId: batchId, dataObjectTypeName: dataObjectTypeName);
                        break;
                    default:
                        throw new NotImplementedException($"Unknown change type {message.ChangeType}");
                }

                await TableService.UpsertObjectAsync(ChangesHandledTableName, message.DihKey, Enum.GetName(typeof(DataChangeType), message.ChangeType) ?? message.ChangeType.ToString(), message);
            }
            catch (Exception ex)
            {
                Logger.DihMonitorError(DihMonitorSystemType.DIH, DihMonitorSystemName.DihIngestionChange, DihMonitorSystemType.DIH, DihMonitorSystemName.DihRawChange, exception: ex, batchId: batchId, dataObjectTypeName: dataObjectTypeName);
                throw;
            }
        }

        private Tmessage DeserializeMessage<Tmessage>(string messageText) where Tmessage : IMessage
        {
            var message = JsonConvert.DeserializeObject<Tmessage>(messageText);
            if (message == null) throw new InvalidOperationException($"Error deserializing message as {nameof(Tmessage)}. Message: {messageText}");
            return message;
        }
    }
}
