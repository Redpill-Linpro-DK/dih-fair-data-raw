# DIH.Data.Raw.Functions

## Overview

The DIH.Data.Raw.Functions project is an Azure Functions-based application designed to interface with Azure Service Bus. It acts as the primary entry point for messages related to dihbaseline data, directing them to specific handlers in the DIH.Data.Raw project. This project is crucial for orchestrating the flow of data and ensuring that each part of the data handling process is triggered at the right time.

## Key Components and Workflow

### Azure Functions

 - HeartbeatTimer: A timer-triggered function that keeps the application active and ensures message queues are regularly checked.
 - ReceiveChange: Handle UPSERT, DELETE and PATCH of data items from external callers.
- ReceiveFullBatch: The main gateway for incoming full batch messages. It handles the initial receipt of data files, parses JSON content, and coordinates the import of data objects to the database. It also manages locks on data types to prevent multiple batches to change the same data.
- ReceiveFullBatchPurgePlan: Triggered after all segments of a batch are processed by Receive. It plans the purging of outdated documents based on unchanged IDs and batch information.
- ReceiveFullBatchPurgeExecute: Executes the purging (deletion or soft-deletion) of documents as determined by the PurgePlan.
- ReceiveFullBatchBroadcast: After the purging process, it broadcasts messages about data updates or deletions to a Service Bus topic, informing the DIH.Data.Prepared project.
- ReceiveFullBatchCleanup: Concludes the batch processing workflow by cleaning up temporary tables, moving blob files, and releasing data type locks.
- ReceiveFullBatchAbort: Engaged in error situations or when a purge operation would delete an excessive amount of data. It modifies the data state to ensure future batch processing is accurate.

Note that queue names are specified both as constants and as configuration values due to [a limitation in the consumption plan](https://github.com/Azure/azure-functions-host/issues/7210).

## Configuration and Integration

The project includes host.json and .csproj files, which are crucial for configuring the Azure Functions environment and managing project dependencies.
Integration with Azure Services such as Service Bus, Blob Storage, and potentially others based on the project's needs.

## Helpers

The MessageHandlerHelper.cs, provides support common functionality for message processing functions.

## Change requests - Data Flow and processing
Data enters through the ReceiveChange function as a message. The message specify the data object type name the change relate to, data required for the operation, and the type of operation:
- AddOrUpdate: Does id transformation (if configured) and appends batch information, then does an upsert of the raw data object received. Full data object is expected in message.
- Delete: Require that id and partition keys are part of data sent - provided objects are deleted from the raw database.
- Patch: Require that id and partition keys are part of data sent - merge fields into existing data and upserts the resulting object.

When all data objects have been handled, changes are broadcast. 

## Full Batch - Data Flow and Processing

- Data enters through the ReceiveFullBatch function, which processes and imports it into the system. Messages come from the integration layer.
- The FullBatchPurgePlan and FullBatchPurgeExecute functions manage the removal of outdated data. A soft-delete function is active by default, hard delete can be enabled via configuration.
- The FullBatchBroadcast function communicates updates to other systems, ensuring data consistency across the ecosystem.
- The FullBatchCleanup function resets the environment for new batches.
- The FullBatchAbort function intervenes in cases of errors or potential data integrity issues.

### Error Handling and System Integrity

Error handling is done in a way to maintain overall data integrity. Exceptions thrown during all stages (except cleanup) will call FullBatchReceiveAbort, which will leave raw data in a state, where future batches are guaranteed to broadcast changes upon completion.

## Conclusion

DIH.Data.Raw.Functions serves as the orchestration layer in the data processing pipeline, managing the flow and processing of dihbaseline data from ingestion to the raw database and broadcast of changes. It works in close coordination with the DIH.Data.Raw project, ensuring that data is processed efficiently, accurately, and securely within a cloud-based, serverless architecture.
