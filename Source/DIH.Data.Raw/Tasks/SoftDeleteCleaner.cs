using DIH.Common;
using DIH.Common.Services.Database;
using DIH.Common.Services.Settings;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace DIH.Data.Raw.Tasks
{
    public class SoftDeleteCleaner
    {
        private DihLogger<SoftDeleteCleaner> Logger { get; set; }
        private IConfiguration Configuration { get; set; }
        private IDatabaseService DatabaseService { get; set; }
        private IFunctionsSettingsService FunctionsSettingsService { get; set; }

        public SoftDeleteCleaner(
            ILogger<SoftDeleteCleaner> logger,
            IConfiguration configuration,
            IDatabaseService databaseService,
            IFunctionsSettingsService functionsSettingsService)
        {
            Logger = logger.AsDihLogger();
            Configuration = configuration;
            DatabaseService = databaseService;
            FunctionsSettingsService = functionsSettingsService;
        }

        public async Task Purge()
        {
            try
            {
                var retentionDays = Configuration.GetValue<int>(ConfigKeys.Data_Raw_SoftDeletedRetentionDays, 14);

                var deletedTotal = await PurgeSoftDeleted(retentionDays);

                Logger.DihInformation($"{deletedTotal} 'soft deleted' data objects was permanently purged from the raw database due to being older than {retentionDays} days...");
            }
            catch (Exception e)
            {
                Logger.DihError(e);
                throw;
            }
        }

        private async Task<int> PurgeSoftDeleted(int retentionDays)
        {
            var deleteCountTotal = 0;

            var dataObjectTypeNames = await DatabaseService.GetDataObjectTypeNamesAsync();
            var queryFilter = $"{JsonPropertyNames.DIH_Status} = '{JsonPropertyValues.DIH_Status_SoftDeleted}' AND {JsonPropertyNames.DIH_LastUpdate} < '{DateTime.UtcNow.AddDays(-retentionDays).ToString("o")}'";

            foreach (var dataObjectTypeName in dataObjectTypeNames)
            {
                var idsToPurge = DatabaseService.GetIdentifiersAsync(dataObjectTypeName, queryFilter);
                var deleteCount = await DatabaseService.DeleteBulkAsync(dataObjectTypeName, idsToPurge, FunctionsSettingsService.MaxParallelTasks);
                Logger.DihDebug($"Deleted {deleteCount} objects of {dataObjectTypeName}");

                deleteCountTotal += deleteCount;
            }

            return deleteCountTotal;
        }
    }
}

