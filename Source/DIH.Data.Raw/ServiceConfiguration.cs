using DIH.Common;
using DIH.Common.Services.Database;
using DIH.Common.Services.Messaging;
using DIH.Common.Services.Settings;
using DIH.Common.Services.Storage;
using DIH.Common.Services.Table;
using DIH.Data.Raw.MessageHandlers;
using DIH.Data.Raw.MessageHandlers.Helpers;
using DIH.Data.Raw.Tasks;
using Microsoft.Azure.Functions.Extensions.DependencyInjection;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace DIH.Data.Raw
{
    public static class ServiceConfiguration
    {
        public static void UseDihDataRawServices(this IFunctionsHostBuilder builder)
        {
            // Get config's
            var config = builder.GetContext().Configuration;

            // Register helpers
            builder.Services.AddSingleton<AuditCleaner>();
            builder.Services.AddSingleton<SoftDeleteCleaner>();
            builder.Services.AddSingleton<PayloadHelper, PayloadHelper>();
            builder.Services.AddSingleton<BroadcastHelper, BroadcastHelper>();
            builder.Services.AddSingleton<DataObjectTypeLocker, DataObjectTypeLocker>();

            // Register Storage service
            builder.Services.AddSingleton<IStorageServiceDictionary, StorageServiceDictionary>(service =>
                new StorageServiceDictionary(
                    new Dictionary<Layer, IStorageService>() {
                        {
                            Layer.Ingestion,
                                new AzureStorageBlobService(
                                    service.GetRequiredService<ILogger<AzureStorageBlobService>>(),
                                    config[ConfigKeys.Ingestion_StorageHttpEndpoint] ?? throw new InvalidOperationException($"{ConfigKeys.Ingestion_StorageHttpEndpoint} config not set")
                                    )
                        }
                    }
                )
            );

            // Register Database service
            builder.Services.AddSingleton<IDatabaseService, AzureCosmosDatabaseService>(service =>
                new AzureCosmosDatabaseService(
                    service.GetRequiredService<ILogger<AzureCosmosDatabaseService>>(),
                    service.GetRequiredService<IConfiguration>(),
                    config[ConfigKeys.Data_Raw_CosmosAccountEndpoint] ?? throw new InvalidOperationException($"{ConfigKeys.Data_Raw_CosmosAccountEndpoint} config not set"),
                    Layer.DataRaw
                    )
            );

            // Register Table service
            builder.Services.AddSingleton<ITableService, AzureTableStorageService>(service =>
                new AzureTableStorageService(
                    config[ConfigKeys.Data_Raw_TableServiceEndpoint] ?? throw new InvalidOperationException($"{ConfigKeys.Data_Raw_TableServiceEndpoint} config not set")
                    )
            );

            // Register Queue service
            builder.Services.AddSingleton<IMessagingServiceDictionary, MessagingServiceDictionary>(service =>
                new MessagingServiceDictionary(
                    new Dictionary<Layer, IMessagingService>() {
                        {
                            Layer.DataRaw,
                            new AzureServiceBusQueueService(
                                service.GetRequiredService<ILogger<AzureServiceBusQueueService>>(),
                                config[ConfigKeys.Data_Raw_ServiceBusHttpEndpoint] ?? throw new InvalidOperationException($"{ConfigKeys.Data_Raw_ServiceBusHttpEndpoint} config not set")
                            )
                        }
                    }
                )
            );

            // Register Function Settings service
            builder.Services.AddSingleton<IFunctionsSettingsService, ConfigBasedFunctionsSettingsService>();

            // Message handlers 
            builder.Services.AddSingleton<IMessageHandler, ReceiveFullBatchMessageHandler>();
            builder.Services.AddSingleton<IMessageHandler, ReceiveFullBatchPurgePlanMessageHandler>();
            builder.Services.AddSingleton<IMessageHandler, ReceiveFullBatchPurgeExecuteMessageHandler>();
            builder.Services.AddSingleton<IMessageHandler, ReceiveFullBatchBroadcastMessageHandler>();
            builder.Services.AddSingleton<IMessageHandler, ReceiveFullBatchCleanupMessageHandler>();
            builder.Services.AddSingleton<IMessageHandler, ReceiveFullBatchAbortMessageHandler>();
            builder.Services.AddSingleton<IMessageHandler, ReceiveChangeMessageHandler>();
        }
    }
}

