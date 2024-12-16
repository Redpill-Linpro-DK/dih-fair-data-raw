# Data Raw Configuration

This sub-system use the below configuration keys.

To ensure a Function App use recent configuration changes, make sure to restart the all instances that should see the changes immediately.

The default numbers for concurrent Service Bus calls, max parallel tasks and tasks per message has been sized to fit a consumption plan deployment.

## host.json

Update the `host.json` file for the Data Raw Azure Function to control the following:
- `logging.LogLevel` to control what is logged during execution
- `extensions.serviceBus.maxConcurrentCalls` to control maximum number of messages that can run at once

host.json also let you manage other aspects relating to Service Bus, Application Insights and general Azure Function behavior.

## Function App local configuration keys

The following keys MUST live with the Function App.

`AzureAppConfigurationEndpoint`
Explains the HTTP endpoint used for accessing the App Configuration service that hold.
Example:  `https://config-dih-dihbaseline-dev-bee5-01.azconfig.io`

`APPINSIGHTS_INSTRUMENTATIONKEY`
Instrumentation key for Application Insights.

## App Configuration service 

The below keys are located with the deployments common App Configuration service.

`Data:Raw:IdSubstitute:*`
Optional. Allow you to add a calculated `id` field to ingested documents that need it. Replace * with a Data Object Type Name from the Integration layer. [See documentation](./Dynamic-ID-Generation.md). 
Define this value via /Source/Schemas/domain-objects.yml in the Common repo.

`Data:Raw:PartitionKey:*`
Define the partition key field for a data type (data object type name as *). This is used when accessing data and will, together with id, identify a unique data object.
Define this value via /Source/Schemas/domain-objects.yml in the Common repo.

`DIH:Developer:OnlyUseStandardConnections`
Default empty.
When true connections (ex Cosmos) will use standard protocols. Set to true when debugging from behind a restrictive firewall.

`Data:Raw:SoftDelete`
Default `true`.
When true documents in the raw database will be 'soft deleted' - they will stay but have property `__DIH_Status = "SOFT_DELETED"`. Enabling this feature make the raw database more robust against corruption and is recommended.

`DIH:Functions:MaxParallelTasks`
Default value: `25`
Specifies the maximum number of parallel tasks that can be executed concurrently by a DIH Function.

`DIH:Functions:ResourceIntensive:MaxParallelTasks`
Default value: `9`
Sets the limit for the maximum number of parallel tasks for resource-intensive operations within a DIH Function.

`DIH:Functions:MaxInMemObjects`
Default value: `500`
Defines the maximum number of objects that can be held in memory at any given time by a DIH Function.

`DIH:Functions:MaxTasksPerMessage`
Default value: `250`
Determines the maximum number of tasks that can be processed from a single message within a DIH Function.

`DIH:Functions:BatchTimeoutSeconds`
Default value: `3600` (1 hour)
Specifies the time in seconds before a batch operation times out.

`DIH:Functions:MessageTTLSeconds`
Default value: `36000` (10 hours)
Indicates the time-to-live for a message in seconds, after which the message expires if not processed.

`DIH:Functions:CancelFullBatchOnException`
Default value: `True`
Indicates whether to cancel the entire batch of operations if an exception occurs during processing.

`Data:Raw:MaxDeletePercent`
Default value: `100`
The maximum percentage of objects that a batch is allowed to delete. Batch is aborted (during dataraw-receive-fullbatch-purge-plan stage) if above this percentage.

`Data:Raw:MaxDeletePercent:*`
Like Data:Raw:MaxDeletePercent, but for a specific Data Object Type - replace * with the name. Setting this will override the number for the specified type.

`Data:Raw:HistoryRetentionDays`
Default value: `14`
Batch run audit data and files older than X days gets deleted. Increase this to keep data for longer.

`Data:Raw:SoftDeletedRetentionDays`
Default value: `14`
Soft deleted database objects older than X days gets permanently deleted. Increase this to keep data for longer.

`Data:Raw:StorageHttpEndpoint`
Explains the HTTP endpoint used for accessing raw data storage.

`Data:Raw:ServiceBus:fullyQualifiedNamespace`
Indicates the fully qualified namespace of the Service Bus used for raw data.

`Data:Raw:CosmosAccountEndpoint`
Describes the endpoint for the Cosmos DB account associated with raw data handling.

`Data:Raw:TableServiceEndpoint`
Explains the endpoint for the table service within the raw data storage context.

`Data:Raw:ReceiveFullBatch:QueueName`
The name of the queue for receiving raw data.

`Data:Raw:ReceiveFullBatchPurgePlan:QueueName`
The queue name designated for receiving purge plans.

`Data:Raw:ReceiveFullBatchPurgeExecute:QueueName`
This is the name of the queue for receiving purge execution commands.

`Data:Raw:ReceiveFullBatchBroadcast:QueueName`
Defines the queue name for receiving broadcast messages.

`Data:Raw:ReceiveFullBatchCleanup:QueueName`
The queue name used for receiving cleanup messages.

`Data:Raw:ReceiveFullBatchAbort:QueueName`
The queue name used for receiving batch abort messages.

`Data:Raw:Table:BatchesHandled`
Represents the name of the table storage used for tracking handled batches.

`Data:Raw:Table:UnchangedIds`
The table name for storing IDs that have not changed during processing.

`Data:Raw:Table:DeletedIds`
Specifies the table for keeping track of deleted IDs.

`Data:Raw:Table:ImportLog`
The name of the table used for logging import operations.

`Data:Raw:Table:ActiveBatches`
Denotes the table used for tracking active batches in the system.

`Data:Raw:Table:CanceledBatches`
This table name is used to keep a log of batches that have been canceled.

