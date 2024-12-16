using DIH.Common;
using DIH.Common.Services.Messaging;
using DIH.Common.Services.Messaging.Base;
using DIH.Common.Services.Settings;
using DIH.Common.Services.Table;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace DIH.Data.Raw.MessageHandlers.Helpers
{
    public class LockResult
    {
        public bool LockGranted { get; set; }
        public bool NewLock { get; set; }

        public LockResult(bool lockGranted, bool newLock)
        {
            LockGranted = lockGranted;
            NewLock = newLock;
        }
    }

    public class DataObjectTypeLocker
    {
        private DihLogger<DataObjectTypeLocker> Logger { get; set; }
        private ITableService TableService { get; set; }
        private IMessagingService DataRawMessageService { get; set; }
        private IFunctionsSettingsService FunctionsSettingsService { get; set; }
        private string ActiveBatchesTableName { get; set; }
        private string CanceledBatchesTableName { get; set; }
        private string ReceiveAbortQueueName { get; set; }


        public DataObjectTypeLocker(
            ILogger<DataObjectTypeLocker> logger,
            IConfiguration configuration,
            ITableService tableService,
            IMessagingServiceDictionary messageServiceDictionary,
            IFunctionsSettingsService functionsSettingsService)
        {
            Logger = logger.AsDihLogger();
            TableService = tableService;
            DataRawMessageService = messageServiceDictionary.Get(Layer.DataRaw);
            FunctionsSettingsService = functionsSettingsService;
            ActiveBatchesTableName = configuration[ConfigKeys.Data_Raw_TableActiveBatches] ?? throw new InvalidOperationException($"Missing config {ConfigKeys.Data_Raw_TableActiveBatches}");
            CanceledBatchesTableName = configuration[ConfigKeys.Data_Raw_TableCanceledBatches] ?? throw new InvalidOperationException($"Missing config {ConfigKeys.Data_Raw_TableCanceledBatches}");
            ReceiveAbortQueueName = configuration[ConfigKeys.Data_Raw_ReceiveFullBatch_Abort_QueueName] ?? throw new InvalidOperationException($"Missing config {ConfigKeys.Data_Raw_ReceiveFullBatch_Abort_QueueName}");
        }

        public async Task VerifyLock(IIngestionMessage lockHoldersMessage)
        {
            var hasLock = await TableService.ExistsAsync(ActiveBatchesTableName, lockHoldersMessage.DataObjectTypeName, lockHoldersMessage.DihKey);

            if (!hasLock)
            {
                throw new AbortEntireBatchException($"Lock lost for {lockHoldersMessage.DataObjectTypeName} {lockHoldersMessage.DihKey}.");
            }
        }

        public async Task<LockResult> GetDataObjectTypeLock(IngestionFullBatchMessage message)
        {
            var currentLockMessage = await TableService.GetObjectsAsync<DataObjectTypeLockMessage>(ActiveBatchesTableName, message.DataObjectTypeName).FirstOrDefaultAsync();
            if (currentLockMessage != null)
            {
                if (currentLockMessage.BatchId == message.BatchId)
                {
                    // All good - the existing lock is for this batch
                    return new LockResult(lockGranted: true, newLock: false);
                }
                else
                {
                    // Another batch has a lock ...
                    TimeSpan lockAge = DateTime.Now - currentLockMessage.Created;

                    if (lockAge > FunctionsSettingsService.BatchTimeout)
                    {
                        // Existing lock is too old - we stop it and clean it up
                        Logger.LogWarning($"The batch {currentLockMessage.DihKey} did not finish within {FunctionsSettingsService.BatchTimeout} and will be aborted.:\n{JsonConvert.SerializeObject(currentLockMessage, Formatting.Indented)}");

                        // Remove old lock
                        await RemoveLock(currentLockMessage, markBatchAsCanceled: true);

                        // Clean up after old lock
                        await SendBatchToAbortQueue(currentLockMessage);

                        // Create new lock for this batch
                        return await CreateLock(message);
                    }
                    else
                    {
                        return new LockResult(lockGranted: false, newLock: false);
                    }
                }
            }
            else
            {
                // Check canceled table - this could be a canceled batch coming back
                await ThrowIfCanceled(message);

                return await CreateLock(message);
            }
        }

        private async Task SendBatchToAbortQueue(DataObjectTypeLockMessage currentLockMessage)
        {
            var abortMessage = new IngestionFullBatchMessage()
            {
                DataObjectTypeName = currentLockMessage.DataObjectTypeName,
                BatchId = currentLockMessage.BatchId
            };
            await DataRawMessageService.EnqueueMessageAsync(ReceiveAbortQueueName, abortMessage);
        }

        public async Task RemoveLock(IngestionFullBatchMessage lockHoldersMessage, bool markBatchAsCanceled = false)
        {
            Logger.DihDebug($"Removing {lockHoldersMessage.DataObjectTypeName} lock for {lockHoldersMessage.BatchId}.");
            var toKillLockMessage = await TableService.TryGetObjectAsync<DataObjectTypeLockMessage>(ActiveBatchesTableName, lockHoldersMessage.DataObjectTypeName, lockHoldersMessage.BatchId);

            if (toKillLockMessage != null)
            {
                await TableService.DeleteAsync(ActiveBatchesTableName, toKillLockMessage.DataObjectTypeName, toKillLockMessage.BatchId);

                if (markBatchAsCanceled) await SendBatchToAbortQueue(toKillLockMessage);
            }

            if (markBatchAsCanceled)
            {
                Logger.DihDebug($"Marking {lockHoldersMessage.DataObjectTypeName} lock for {lockHoldersMessage.BatchId} as canceled.");
                await TableService.UpsertObjectAsync(CanceledBatchesTableName, lockHoldersMessage.DataObjectTypeName, lockHoldersMessage.BatchId, lockHoldersMessage);

            }
        }

        public async Task HandleLockDeniedAsync(IngestionFullBatchMessage batchFileMessage, string resendToQueueName)
        {
            var messageAge = DateTime.Now - batchFileMessage.Created;
            if (messageAge < FunctionsSettingsService.MessageTTL)
            {
                // Calculate a schedule that prioritize 1 messages from other batches to arrive at the same time, so we have high degree chance to of handling batches in the order they came
                DateTimeOffset schedule = NextTimeInterval(TimeSpan.FromSeconds(15)) + TimeSpan.FromSeconds(batchFileMessage.BatchSegment == 1 ? 0 : 1);
                Logger.LogWarning($"Cannot start batch {batchFileMessage.BatchId} yet - another batch has a lock on {batchFileMessage.DataObjectTypeName}. Will retry at {schedule}.");
                Logger.LogDebug(JsonConvert.SerializeObject(batchFileMessage, Formatting.Indented));
                // put this message back in the queue
                await DataRawMessageService.EnqueueMessageAsync(resendToQueueName, batchFileMessage, scheduledEnqueueTime: schedule);
            }
            else
            {
                Logger.LogError($"Queue message for {batchFileMessage.DataObjectTypeName} batch {batchFileMessage.BatchId} timed out after {messageAge}. The following batch message is dropped:");
                Logger.LogError(JsonConvert.SerializeObject(batchFileMessage, Formatting.Indented));
            }
        }



        private async Task ThrowIfCanceled(IngestionFullBatchMessage message)
        {
            if (await TableService.ExistsAsync(CanceledBatchesTableName, message.DataObjectTypeName, message.BatchId))
            {
                throw new AbortEntireBatchException($"Canceled batch detected for {message.DataObjectTypeName} {message.BatchId} - this batch id is not allowed to run anymore.");
            }
        }

        // Calculate a future time in a discrete inverval
        private DateTimeOffset NextTimeInterval(TimeSpan jump)
        {
            long ticks = (DateTimeOffset.UtcNow.Ticks + jump.Ticks - 1) / jump.Ticks * jump.Ticks;
            return new DateTimeOffset(ticks, TimeSpan.Zero);
        }


        private async Task<LockResult> CreateLock(IngestionFullBatchMessage processMessage)
        {
            var lockMessage = new DataObjectTypeLockMessage(processMessage);

            // Get a lock. Avoid another batch is racing us.
            await TableService.UpsertObjectAsync(ActiveBatchesTableName, lockMessage.DataObjectTypeName, lockMessage.BatchId, lockMessage);

            // Avoid another batch is racing us.
            await Task.Delay(750);

            var locks = await TableService.GetObjectsAsync<DataObjectTypeLockMessage>(ActiveBatchesTableName, lockMessage.DataObjectTypeName).ToListAsync();

            if (locks.Count > 1)
            {
                Func<IngestionFullBatchMessage, string> sortableFunc = (message) => $"{message.Created.ToString("yyyy-MM-dd HH:mm:ss.fff")}_{message.BatchId}_{message.BatchSegment}";
                Func<IngestionFullBatchMessage, IngestionFullBatchMessage, bool> cameBefore = (m1, m2) => string.Compare(sortableFunc(m1), sortableFunc(m2)) < 0;

                var canHaveLock = !locks.Any(l => l.HasLock);
                canHaveLock = canHaveLock && !locks.Any(l => cameBefore(l, lockMessage));

                if (!canHaveLock)
                {
                    await TableService.DeleteAsync(ActiveBatchesTableName, lockMessage.DataObjectTypeName, lockMessage.BatchId);
                    return new LockResult(lockGranted: false, newLock: false);
                }
            }

            lockMessage.HasLock = true;
            await TableService.UpsertObjectAsync(ActiveBatchesTableName, lockMessage.DataObjectTypeName, lockMessage.BatchId, lockMessage);

            Logger.DihDebug($"Lock for {lockMessage.DataObjectTypeName} given to {lockMessage.BatchId}");
            return new LockResult(lockGranted: true, newLock: true);
        }

        public class DataObjectTypeLockMessage : IngestionFullBatchMessage
        {
            public bool HasLock { get; set; }

            public DataObjectTypeLockMessage() { }

            public DataObjectTypeLockMessage(IngestionFullBatchMessage sourceMessage) : base(
                sourceMessage.DataObjectTypeName,
                sourceMessage.BatchId,
                sourceMessage.BatchSegment,
                sourceMessage.BatchSegmentsTotal)
            {
                Created = sourceMessage.Created;
                Completed = sourceMessage.Completed;
                CompletedTime = sourceMessage.CompletedTime;
                HasLock = false;
            }
        }

    }
}

