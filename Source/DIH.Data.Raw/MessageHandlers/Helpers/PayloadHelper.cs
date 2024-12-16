using DIH.Common;
using DIH.Common.Json;
using DIH.Common.Json.DeserializerAppendVisitors;
using DIH.Common.Json.DeserializerObjectNavigators;
using DIH.Common.Services.Database;
using DIH.Common.Services.Messaging.Base;
using DIH.Common.Services.Settings;
using DIH.Common.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;
using System.Collections.Concurrent;

namespace DIH.Data.Raw.MessageHandlers.Helpers
{
    public class PayloadHelper
    {
        private DihLogger<PayloadHelper> Logger { get; set; }
        private IConfiguration Configuration { get; set; }
        private IDatabaseService DatabaseService { get; set; }
        private IFunctionsSettingsService FunctionsSettingsService { get; set; }
        private bool SoftDeleteStale { get; set; }

        public PayloadHelper(
            ILogger<PayloadHelper> logger,
            IConfiguration configuration,
            IDatabaseService databaseService,
            IFunctionsSettingsService functionsSettingsService)
        {
            Logger = logger.AsDihLogger();
            FunctionsSettingsService = functionsSettingsService;
            Configuration = configuration;
            DatabaseService = databaseService;

            SoftDeleteStale = bool.Parse(configuration[ConfigKeys.Data_Raw_SoftDelete] ?? throw new InvalidOperationException($"Missing config {ConfigKeys.Data_Raw_SoftDelete}"));
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="payloadStream">List of objects to upsert if changed or new</param>
        /// <param name="batchId">String that identify current bacth on upserted data objects</param>
        /// <param name="message">The message relating to the upsert</param>
        /// <param name="unchangedIdsHandler">Function to call with list of unchanged IDs</param>
        /// <returns></returns>
        public async Task<int> UpsertChangesFromStream(Stream payloadStream, IIngestionMessage message, Func<IEnumerable<DataObjectIdentifier>, Task> changedIdsHandler, Func<IEnumerable<DataObjectIdentifier>, Task> unchangedIdsHandler)
        {
            var dataObjects = GetRawObjectsFromStream(payloadStream, message);
            return await UpsertChanges(dataObjects, message, changedIdsHandler, unchangedIdsHandler);
        }

        public IAsyncEnumerable<JObject> GetRawObjectsFromStream(Stream objectStream, IIngestionMessage message)
        {
            // Configure de-serializer - note: visitor order matters!
            var idSubstituteRuleString = Configuration[ConfigKeys.Data_Raw_IdSubstitute_PREFIX + message.DataObjectTypeName];
            var rootPropertyArrayNavigator = new RootPropertyArrayObjectNavigator();
            var deserializerVisitors = new List<IDeserializerAppendVisitor>
            {
                new IdTransformationAppender(idSubstituteRuleString, moveIdToTop: true),
                new DihHashAppender(),
                new DihStatusAppender(JsonPropertyValues.DIH_Status_Active),
                new DihBatchIdAppender(message.BatchId),
                new DihImportTimeAppender()
            };

            // Build de-serializer
            return objectStream.DeserializeJsonAsync<JObject>(
                    objectNavigator: rootPropertyArrayNavigator,
                    deserializerVisitors: deserializerVisitors);
        }


        public async Task PatchChangesFromStream(IIngestionMessage message, IAsyncEnumerable<JObject> patchObjects, Func<IEnumerable<DataObjectIdentifier>, Task> patchedIdsHandler)
        {
            int totalCount = 0;
            var patchedIds = new ConcurrentBag<DataObjectIdentifier>();

            await using (var bulkAwaiter = new BulkTaskAwaiter(awaitAtMaxTasks: FunctionsSettingsService.MaxParallelTasks))
            {
                await foreach (var patchObject in patchObjects)
                {
                    totalCount += 1;
                    var patchTask = new Func<Task>(async () =>
                    {
                        JProperty? idProperty = patchObject.Property(JsonPropertyNames.Id);
                        if (idProperty != null)
                        {
                            DataObjectIdentifier dataObjectIdentifier = DatabaseService.GetDataObjectIdentifier(message.DataObjectTypeName, patchObject);
                            var sourceObject = await DatabaseService.GetByIdentifierAsync<JObject>(message.DataObjectTypeName, dataObjectIdentifier);
                            bool hasChanges = false;
                            if (sourceObject != null)
                            {
                                var sourcePropertyNames = sourceObject.Properties().Select(p => p.Name);
                                hasChanges = patchObject.Properties().Select(p => p.Name).Any(patchPropertyName => !sourcePropertyNames.Contains(patchPropertyName));
                                foreach (var sourcePropertyName in sourcePropertyNames.Where(propName => !JsonPropertyNames.DIH_Properties.Contains(propName)))
                                {
                                    if (patchObject.Property(sourcePropertyName) == null)
                                    {
                                        patchObject[sourcePropertyName] = sourceObject[sourcePropertyName];
                                    }
                                    else
                                    {
                                        hasChanges = hasChanges || patchObject.Value<string>(sourcePropertyName) != sourceObject.Value<string>(sourcePropertyName);
                                    }
                                }
                            }
                            else
                            {
                                hasChanges = true;
                            }

                            if (hasChanges)
                            {
                                await DatabaseService.UpsertAsync(message.DataObjectTypeName, patchObject);
                                patchedIds.Add(dataObjectIdentifier);
                            }
                        }
                        else
                        {
                            throw new InvalidOperationException($"Missing {JsonPropertyNames.Id} on payload object {patchObject} - check documentation for 'Dynamic ID Generation' using config key 'Data:Raw:IdSubstitute:{message.DataObjectTypeName}'.");
                        }
                    });

                    await bulkAwaiter.Add(patchTask());
                }
            }
            await patchedIdsHandler(patchedIds);
            Logger.DihInformation($"Patch of {totalCount} data objects completed ({patchedIds.Count} patched, {totalCount - patchedIds.Count} unchanged).");
        }

        public async Task DeleteDataObject(DataObjectIdentifier dataObjectIdntifier, IIngestionMessage message)
        {
            if (SoftDeleteStale)
            {
                var softDeletee = await DatabaseService.GetByIdentifierAsync<JObject>(message.DataObjectTypeName, dataObjectIdntifier);
                if (softDeletee != null)
                {
                    var idProperty = softDeletee.Property(JsonPropertyNames.DIH_Status);
                    if (idProperty != null) idProperty.Remove();
                    softDeletee.AddFirst(new JProperty(JsonPropertyNames.DIH_Status, JsonPropertyValues.DIH_Status_SoftDeleted));
                    softDeletee[JsonPropertyNames.DIH_UpdatingBatchId] = message.BatchId;
                    softDeletee[JsonPropertyNames.DIH_LastUpdate] = DateTime.Now;
                    await DatabaseService.UpsertAsync(message.DataObjectTypeName, softDeletee);
                }
            }
            else
            {
                await DatabaseService.DeleteAsync(message.DataObjectTypeName, dataObjectIdntifier);
            }
        }

        /// <summary>
        /// Upserts changed data. The ID of unchánged data is passed to the supplied unchangedIdsHandler
        /// </summary>
        /// <param name="dataObjects">List of objects to upsert if changed or new</param>
        /// <param name="message">The message relating to the upsert</param>
        /// <param name="unchangedIdsHandler">Function to call with list of unchanged IDs</param>
        /// <returns>Number of data objects that was upserted (changed or new)</returns>
        private async Task<int> UpsertChanges(IAsyncEnumerable<JObject> dataObjects, IIngestionMessage message, Func<IEnumerable<DataObjectIdentifier>, Task> changedIdsHandler, Func<IEnumerable<DataObjectIdentifier>, Task> unchangedIdsHandler)
        {
            Logger.DihDebug($"Loop through data objects from JSON and ingest them in bulks of {FunctionsSettingsService.MaxInMemObjects}");
            var upsertCount = 0;
            var totalCount = 0;
            var dataObjectChunk = new List<JObject>();
            await foreach (var dataObject in dataObjects)
            {
                dataObjectChunk.Add(dataObject);
                if (dataObjectChunk.Count == FunctionsSettingsService.MaxInMemObjects)
                {
                    upsertCount += await IngestSubset(dataObjectChunk, message, changedIdsHandler, unchangedIdsHandler);
                    dataObjectChunk.Clear();
                }
                totalCount += 1;
            }
            upsertCount += await IngestSubset(dataObjectChunk, message, changedIdsHandler, unchangedIdsHandler);

            Logger.DihInformation($"Import of {totalCount} data objects completed ({upsertCount} upserted, {totalCount - upsertCount} unchanged).");

            return upsertCount;
        }

        private async Task<int> IngestSubset(IList<JObject> dataObjectChunk, IIngestionMessage batchFileMessage, Func<IEnumerable<DataObjectIdentifier>, Task> changedIdsHandler, Func<IEnumerable<DataObjectIdentifier>, Task> unchangedIdsHandler)
        {
            if (dataObjectChunk.Count == 0) { return 0; }

            Logger.DihDebug($"Ingesting chunk of {dataObjectChunk.Count} data objects.");

            string partitionKeyConfigName = ConfigKeys.DataPartitionKey(Layer.DataRaw, batchFileMessage.DataObjectTypeName);
            var partitionKeyName = Configuration[partitionKeyConfigName];
            if (string.IsNullOrEmpty(partitionKeyName)) throw new InvalidOperationException($"Partition Key name for data object type '{batchFileMessage.DataObjectTypeName}' not found in config '{partitionKeyConfigName}' - this valie should have been created when running the Common pipeline.");

            Func<JObject, string> partitionKeyValueGet = (dataObject) =>
            {
                var partitionKey = dataObject.Value<string>(partitionKeyName);
                if (string.IsNullOrWhiteSpace(partitionKey)) throw new InvalidDataException($"Missing value for partition key '{partitionKeyName}' for data with id '{dataObject.IdValue()}'");
                return partitionKey;
            };

            string idHashQueryFilter = string.Join(" OR ",
                dataObjectChunk.Select(dataObject =>
                    $"{JsonPropertyNames.Id} = '{dataObject.IdValue()}' " +
                    $"AND {partitionKeyName} = '{partitionKeyValueGet(dataObject)}' " +
                    $"AND {JsonPropertyNames.DIH_Hash} = '{dataObject.HashValue()}' " +
                    $"AND {JsonPropertyNames.DIH_Status} = '{JsonPropertyValues.DIH_Status_Active}'"));

            var unchangedIdentifiers = await DatabaseService.GetIdentifiersAsync(batchFileMessage.DataObjectTypeName, idHashQueryFilter).ToListAsync();

            var newOrChanged = dataObjectChunk.Where(o => !unchangedIdentifiers.Any(u => u.Id == o.IdValue() && u.PartitionKey == o.Value<string>(partitionKeyName)));

            // Update database 
            var upsertCount = await DatabaseService.UpsertBulkAsync(batchFileMessage.DataObjectTypeName, newOrChanged.ToAsyncEnumerable(), FunctionsSettingsService.MaxParallelResourceIntensiveTasks);

            // Handle list of unchanged IDs as per callers requirements
            await unchangedIdsHandler(unchangedIdentifiers);
            await changedIdsHandler(newOrChanged.Select(o => DatabaseService.GetDataObjectIdentifier(batchFileMessage.DataObjectTypeName, o)));

            Logger.DihDebug($"Segment chunk of {dataObjectChunk.Count} data objects processed ({upsertCount} upserted, {unchangedIdentifiers.Count} unchanged).");

            return upsertCount;
        }
    }
}
