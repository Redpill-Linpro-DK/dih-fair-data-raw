using DIH.Common;
using DIH.Common.Services.Messaging;
using DIH.Common.Services.Settings;
using DIH.Common.Services.Storage;
using DIH.Common.Services.Table;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace DIH.Data.Raw.Tasks
{
    public class AuditCleaner
    {
        private DihLogger<AuditCleaner> Logger { get; set; }
        private IConfiguration Configuration { get; set; }
        private ITableService TableService { get; set; }
        private IStorageServiceDictionary StorageServiceDictionary { get; set; }
        private IFunctionsSettingsService FunctionsSettingsService { get; set; }

        public AuditCleaner(
            ILogger<AuditCleaner> logger,
            IConfiguration configuration,
            ITableService tableService,
            IStorageServiceDictionary storageServiceDictionary,
            IFunctionsSettingsService functionsSettingsService)
        {
            Logger = logger.AsDihLogger();
            Configuration = configuration;
            TableService = tableService;
            FunctionsSettingsService = functionsSettingsService;
            StorageServiceDictionary = storageServiceDictionary;
        }

        public async Task Purge()
        {
            try
            {
                int retentionDays = Configuration.GetValue<int>(ConfigKeys.Data_Raw_HistoryRetentionDays, 14);

                await PurgeFullBatchStorage(retentionDays);
                await PurgeChangesStorage(retentionDays);
                await PurgeTaskHistory(retentionDays);
                await PurgeTempTables();
            }
            catch (Exception e)
            {
                Logger.DihError(e);
                throw;
            }
        }

        private async Task PurgeTempTables()
        {
            string[] tablesForTempState = {
                    Configuration[ConfigKeys.Data_Raw_TableUnchangedIds] ?? throw new InvalidOperationException($"Missing config {ConfigKeys.Data_Raw_TableUnchangedIds}"),
                    Configuration[ConfigKeys.Data_Raw_TableDeletedIds] ?? throw new InvalidOperationException($"Missing config {ConfigKeys.Data_Raw_TableDeletedIds}"),
                    Configuration[ConfigKeys.Data_Raw_TableActiveBatches] ?? throw new InvalidOperationException($"Missing config {ConfigKeys.Data_Raw_TableActiveBatches}")
                };

            foreach (var tableName in tablesForTempState)
            {
                var deletecCount = await TableService.DeleteOldRecordsAsync(tableName, TimeSpan.FromDays(1), FunctionsSettingsService.MaxParallelTasks);
                if (deletecCount > 0) Logger.DihWarning($"Cleaned up {deletecCount} rows from temporary table {tableName} - table was not cleaned up post-batch as expected.");
                else Logger.DihDebug($"Cleaned up {deletecCount} rows from temporary table {tableName}.");
            }
        }

        private async Task PurgeTaskHistory(int retentionDays)
        {
            string[] tablesForHistoricEvents = {
                    Configuration[ConfigKeys.Data_Raw_TableCanceledBatches] ?? throw new InvalidOperationException($"Missing config {ConfigKeys.Data_Raw_TableCanceledBatches}"),
                    Configuration[ConfigKeys.Data_Raw_TableImportLogs] ?? throw new InvalidOperationException($"Missing config {ConfigKeys.Data_Raw_TableImportLogs}"),
                    Configuration[ConfigKeys.Data_Raw_TableBatchesHandled] ?? throw new InvalidOperationException($"Missing config {ConfigKeys.Data_Raw_TableBatchesHandled}"),
                    Configuration[ConfigKeys.Data_Raw_TableChangesHandled] ?? throw new InvalidOperationException($"Missing config {ConfigKeys.Data_Raw_TableChangesHandled}")
                };

            foreach (var tableName in tablesForHistoricEvents)
            {
                var deletecCount = await TableService.DeleteOldRecordsAsync(tableName, TimeSpan.FromDays(retentionDays), FunctionsSettingsService.MaxParallelTasks);
                Logger.DihDebug($"Cleaned up {deletecCount} rows from historic run table {tableName} (expected).");
            }
        }

        private async Task PurgeFullBatchStorage(int retentionDays)
        {
            // Grab old from TableBatchesHandled 
            var batchesHandledTableName = Configuration[ConfigKeys.Data_Raw_TableBatchesHandled] ?? throw new InvalidOperationException($"Missing config {ConfigKeys.Data_Raw_TableBatchesHandled}");
            var batchFileMessages = TableService.GetOldObjectsAsync<IngestionFullBatchFileMessage>(batchesHandledTableName, TimeSpan.FromDays(retentionDays));

            // Delete blobs
            await foreach (var batchFileMessage in batchFileMessages)
            {
                var storageService = StorageServiceDictionary.Get(batchFileMessage.StorageLayer);

                try
                {
                    await storageService.DeleteAsync(batchFileMessage.StorageContainerName, batchFileMessage.StoragePath);
                    Logger.DihDebug($"Deleted blob {batchFileMessage.StoragePath}");
                }
                catch (Exception ex)
                {
                    Logger.DihWarning($"Could not clean up blob, probably already moved. {ex.Message}");
                }
            }
        }

        private async Task PurgeChangesStorage(int retentionDays)
        {
            // Grab old from TableBatchesHandled 
            var changesHandledTableName = Configuration[ConfigKeys.Data_Raw_TableChangesHandled] ?? throw new InvalidOperationException($"Missing config {ConfigKeys.Data_Raw_TableChangesHandled}");
            var changesHandlesMessages = TableService.GetOldObjectsAsync<IngestionChangeMessage>(changesHandledTableName, TimeSpan.FromDays(retentionDays));

            // Delete blobs
            await foreach (var changesHandlesMessage in changesHandlesMessages)
            {
                await changesHandlesMessage.DeletePayload(StorageServiceDictionary);
            }
        }

    }
}

