using DIH.Common;
using DIH.Common.Services.Database;
using DIH.Common.Services.Messaging;
using DIH.Common.Services.Messaging.Base;
using DIH.Common.Services.Settings;
using DIH.Common.Services.Storage;
using DIH.Common.Services.Table;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace DIH.Data.Raw.MessageHandlers.Helpers
{
    public abstract class ReceiveFullbatchFunctionBase<T> : IMessageHandler where T : class
    {
        protected DihLogger<T> Logger { get; private set; }
        protected IStorageServiceDictionary StorageServiceDictionary { get; private set; }
        protected ITableService TableService { get; private set; }
        protected IDatabaseService DatabaseService { get; private set; }
        protected IMessagingService DataRawMessageService { get; private set; }
        protected IFunctionsSettingsService FunctionsSettingsService { get; private set; }
        protected IConfiguration Configuration { get; private set; }
        protected BatchMessageExecutionCoordinator<T> ExecutionManager { get; private set; }
        protected DataObjectTypeLocker DataObjectTypeLocker { get; set; }

        protected string UnchangedIdsTableName { get; private set; }
        protected string BatchesHandledTableName { get; private set; }
        protected string DeletedIdsTableName { get; private set; }

        // Queue names
        protected string ReceiveFullbatchQueueName { get; set; }
        protected string ReceiveCleanupQueueName { get; set; }
        protected string ReceivePurgeExecuteQueueName { get; set; }
        protected string ReceiveBroadcastQueueName { get; set; }
        protected string ReceiveAbortQueueName { get; set; }
        protected string ReceivePurgePlanQueueName { get; set; }


        protected ReceiveFullbatchFunctionBase(
            ILogger<T> logger,
            IConfiguration configuration,
            IStorageServiceDictionary storageServiceDictionary,
            ITableService tableService,
            IDatabaseService databaseService,
            IMessagingServiceDictionary messageServiceDictionary,
            IFunctionsSettingsService functionsSettingsService,
            DataObjectTypeLocker dataObjectTypeLocker)
        {
            Logger = logger.AsDihLogger();
            Configuration = configuration;
            StorageServiceDictionary = storageServiceDictionary;
            TableService = tableService;
            DatabaseService = databaseService;
            DataRawMessageService = messageServiceDictionary.Get(Layer.DataRaw);
            FunctionsSettingsService = functionsSettingsService;

            ExecutionManager = new BatchMessageExecutionCoordinator<T>(Logger, Configuration, TableService, FunctionsSettingsService);
            DataObjectTypeLocker = dataObjectTypeLocker;

            // Initialize common table names here if applicable
            UnchangedIdsTableName = configuration[ConfigKeys.Data_Raw_TableUnchangedIds] ?? throw new InvalidOperationException($"Missing config {ConfigKeys.Data_Raw_TableUnchangedIds}");
            BatchesHandledTableName = configuration[ConfigKeys.Data_Raw_TableBatchesHandled] ?? throw new InvalidOperationException($"Missing config {ConfigKeys.Data_Raw_TableBatchesHandled}");
            DeletedIdsTableName = configuration[ConfigKeys.Data_Raw_TableDeletedIds] ?? throw new InvalidOperationException($"Missing config {ConfigKeys.Data_Raw_TableDeletedIds}");

            // Queue names (used for intenal processing)
            ReceiveFullbatchQueueName = configuration[ConfigKeys.Data_Raw_ReceiveFullbatch_QueueName] ?? throw new InvalidOperationException($"Missing config {ConfigKeys.Data_Raw_ReceiveFullbatch_QueueName}");
            ReceivePurgeExecuteQueueName = configuration[ConfigKeys.Data_Raw_ReceiveFullBatch_PurgeExecute_QueueName] ?? throw new InvalidOperationException($"Missing config {ConfigKeys.Data_Raw_ReceiveFullBatch_PurgeExecute_QueueName}");
            ReceiveBroadcastQueueName = configuration[ConfigKeys.Data_Raw_ReceiveFullBatch_Broadcast_QueueName] ?? throw new InvalidOperationException($"Missing config {ConfigKeys.Data_Raw_ReceiveFullBatch_Broadcast_QueueName}");
            ReceiveAbortQueueName = configuration[ConfigKeys.Data_Raw_ReceiveFullBatch_Abort_QueueName] ?? throw new InvalidOperationException($"Missing config {ConfigKeys.Data_Raw_ReceiveFullBatch_Abort_QueueName}");
            ReceivePurgePlanQueueName = configuration[ConfigKeys.Data_Raw_ReceiveFullBatch_PurgePlan_QueueName] ?? throw new InvalidOperationException($"Missing config {ConfigKeys.Data_Raw_ReceiveFullBatch_PurgePlan_QueueName}");
            ReceiveCleanupQueueName = configuration[ConfigKeys.Data_Raw_ReceiveFullBatch_Cleanup_QueueName] ?? throw new InvalidOperationException($"Missing config {ConfigKeys.Data_Raw_ReceiveFullBatch_Cleanup_QueueName}");
        }

        protected async Task ExecuteFunctionAsync<Tmessage>(string messageText, Func<Tmessage, Task> processMessageFunc, Func<Tmessage, Task> allMessagesDoneFunc, bool requireDataTypeLock, string ownQueueName) where Tmessage : IngestionFullBatchMessage, new()
        {
            try
            {
                var message = DeserializeMessage<Tmessage>(messageText);
                if (message == null)
                {
                    Logger.DihMonitorError(DihMonitorSystemType.DIH, DihMonitorSystemName.DihRawFullbatchProcessing, DihMonitorSystemType.DIH, DihMonitorSystemName.DihRawChange, message: $"Cannot deserialize message:\n{messageText}");
                    throw new InvalidOperationException($"Unable to deserialize the message.");
                }

                try
                {
                    Logger.DihInformation($"Start processing {ownQueueName} message {message.BatchSegment}/{message.BatchSegmentsTotal} for batch id {message.BatchId}");

                    LockResult? dataTypeLock = null;
                    if (requireDataTypeLock)
                    {
                        dataTypeLock = await DataObjectTypeLocker.GetDataObjectTypeLock(message);
                        if (!dataTypeLock.LockGranted)
                        {
                            await DataObjectTypeLocker.HandleLockDeniedAsync(message, ownQueueName);
                            return;
                        }
                    }

                    bool couldRun = await ExecutionManager.RunOncePerBatchSegmentAsync(
                        message: message,
                        queueName: ownQueueName,
                        primaryTask: processMessageFunc,
                        allDoneTask: allMessagesDoneFunc);

                    if (!couldRun)
                    {
                        Logger.DihError($"Batch {message.DataObjectTypeName} {message.BatchId} message {message.BatchSegment}/{message.BatchSegmentsTotal} dropped.");
                        if (dataTypeLock != null && dataTypeLock.NewLock) await DataObjectTypeLocker.RemoveLock(message, markBatchAsCanceled: false);
                    }

                    Logger.DihInformation($"Done processing {ownQueueName} message {message.BatchSegment}/{message.BatchSegmentsTotal} for batch id {message.BatchId}");
                }
                catch (Exception ex)
                {
                    Logger.DihMonitorError(DihMonitorSystemType.DIH, DihMonitorSystemName.DihRawFullbatchProcessing, DihMonitorSystemType.DIH, DihMonitorSystemName.DihRawChange, exception: ex, batchId: message.BatchId, dataObjectTypeName: message.BatchId);
                    await DataObjectTypeLocker.RemoveLock(message, markBatchAsCanceled: FunctionsSettingsService.CancelFullBatchOnException);
                    throw;
                }
            }
            catch (Exception ex)
            {
                Logger.DihError(ex.Message);

                // Say "handled" to AbortEntireBatchException (by graceful exit) so message never come back
                if (!(ex is AbortEntireBatchException)) throw;
            }
        }

        protected async Task ForwardBatchInfoToQueue(IngestionFullBatchMessage senderMessage, string queueName)
        {
            var broadcastMessage = new IngestionFullBatchMessage()
            {
                DataObjectTypeName = senderMessage.DataObjectTypeName,
                BatchId = senderMessage.BatchId
            };
            Logger.DihInformation($"Sending message to {queueName}");
            await DataRawMessageService.EnqueueMessageAsync(queueName, broadcastMessage);
        }

        private Tmessage DeserializeMessage<Tmessage>(string messageText) where Tmessage : IngestionFullBatchMessage
        {
            var message = JsonConvert.DeserializeObject<Tmessage>(messageText);

            if (message == null) throw new InvalidOperationException($"Error deserializing message as {nameof(Tmessage)}. Message: {messageText}");

            return message;
        }

        public abstract bool CanHandleQueue(string queueName);

        public abstract Task Run(string queueName, string queueMessage);
    }
}

