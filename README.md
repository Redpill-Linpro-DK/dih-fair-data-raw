# DIH.Data.Raw

## Introduction / Motivation

> Infrastructure and code specific to ingestion of, and notifications about, raw data 
> - part of the data layer.
> - data is sent either
>   - as complete sets (in batches) and fully replace existing data
>   - as discrete changes (AddOrUpdate, Delete or Patch)
> - ingests data sent to DihBaseline (via Integration layer) and stores in a database for raw data
> - notifies about changes to raw data via a Service Bus topic

#### Custom setup steps

When the Nuget packages from DIH.Common has been built and published (named DIH.DihBaseline.*), update the following files with the correct version number:
 - [DIH.Data.Raw.csproj](./Source/DIH.Data.Raw/DIH.Data.Raw.csproj)
 - [DIH.Data.Raw.Functions.csproj](./Source/DIH.Data.Raw.Functions/DIH.Data.Raw.Functions.csproj)

At this point, no further customization is required; DIH.Data.Raw can be built and deployed and will be fully functional.

### Using Data Raw

To use this sub-component:
- deploy after the Common repo has deployed.
- use code as is. The code is agnostic towards the data object types being ingested; no code changes are needed to accept a new/changed type of raw data.
- ensure that the Database to Data Raw has containers named exactly like the value of `dataObjectTypeName` in messages sent to this sub-system.
    - the creation of these containers are automatically done by the IaC in the Common repo that know this information (see Common repo, /Source/Schemas)

## Documentation

For an introduction to the C# projects, please see
- [DIH.Data.Raw](./Documentation/DIH.Data.Raw.md).
- [DIH.Data.Raw.Functions](./Documentation/DIH.Data.Raw.Functions.md).

For details about configuring, features or monitoring the system, please see
- [Configuration](./Documentation/Configuration.md).
- [Dynamic ID generation (optional)](./Documentation/Dynamic-ID-Generation.md).
- [Diagnostics](./Documentation/Diagnostics.md).

### The system contains:
- a Azure Functions `dataraw-receive-change` that listens to `ingestion-change` topic via subscription `ingestion-fullbatch-dataraw-subscription`. This will absorb discrete data changes and signal changes to the prepared layer. The queue is fed from the integration layer.
- a Azure Functions `dataraw-receive-fullbatch` that listens to `ingestion-fullbatch` topic via subscription `ingestion-change-dataraw-subscription`. This will start the process of ingesting data. The queue is fed from the integration layer.
- a number of Azure Functions `dataraw-receive-fullbatch-*` that handle steps in the batch processing; delta calculation, change event signalling and cleanup of internal data.
- a number of Storage Tables `RawData*` that keeps track of Data Raw ingestion progress and batch execution.
- a number of Service Bus Queues `dataraw-receive-fullbatch-*` that invoke the Azure Functions with the same name.
- a Azure Function `dataraw-heartbeat-timer` that is time triggered. This ensures the function app is online and can receive messages from queues.
- a Azure Function `dataraw-table-cleanup-timer` that is time triggered. This cleans up table storage. Any residual temporary state is removed after 1 day, while rows from tables storing "run history" are removed after 14 days.

### The system is dependant on the following Common resources:
- Service Bus Queue Topic `dataraw-change` - this topic gets messages when raw data has changed.
- Common Cosmos DB service
- Common Service Bus namespace
- Common App Configuration service
- Common Application Insights service
- Common Storage Account, for reading/moving files sent to `ingestion-change` and `ingestion-fullbatch` topics.

## Implementation

The queue message triggered and time triggered functions are implemented as Azure Functions, located in `/Source/DIH.Data.Raw.Functions`.

Code handling messages are implemented as a class library, located in `/Source/DIH.Data.Raw`.

The following nuget packages are used:
- DIH.DihBaseline.Common
- DIH.DihBaseline.Common.Services
- Azure.Identity
- Microsoft.Azure.Functions.Extensions
- Microsoft.Azure.WebJobs.Extensions.ServiceBus
- Microsoft.Extensions.Configuration.AzureAppConfiguration
- Microsoft.NET.Sdk.Functions

## Deployment

Build, IaC and deployment are handled by `/.azure-pipelines.yml` and Bicep code in `/Source/IaC/*`

## KeyVault
A Global or Local KeyVault can be used:
- Global KeyVault means that the KeyVault is shared with other projects and is located in the common resource group.
- Local KeyVault means that the KeyVault is located in the same resource group as the Function App.

To create the KeyVault and add the RBAC permissions simply change the corresponding boolean in the `azure-pipelines.yml` file.
Next add in the `Startup.cs` file the lines to use the KeyVault:

```csharp
public override void ConfigureAppConfiguration(IFunctionsConfigurationBuilder builder)
{
   builder.UseDihConfiguration();
   builder.UseGlobalKeyVault();
   builder.UseLocalKeyVault();
}
```

Lastly, for local debugging when using the Local KeyVault you need to add the following to the local.settings.json file, replacing the `localKeyVaultName` with the name of the KeyVault:

```json
{
  "Values": {
    "LocalKeyVault:Uri": "https://<localKeyVaultName>.vault.azure.net/"
  }
}
```

## Use of third party libraries
The diagram, images and flowcharts was created in [Draw.io](https://www.draw.io).

## General project information

Please refer to the head repo for general project information.

## Responsible People

| Name | E-mail | Role |
| ---- | ------ | ---- |
| ... | [pm@redpilllinpro.com](mailto:pm@redpilllinpro.com) | Project manager |
| ... | [arc@redpilllinpro.com](mailto:arc@redpilllinpro.com) | Architect |
| ... | [dev@redpilllinpro.com](mailto:dev@redpilllinpro.com) | Developer |
| ... | [dev@redpilllinpro.com](mailto:dev@redpilllinpro.com) | Developer |



