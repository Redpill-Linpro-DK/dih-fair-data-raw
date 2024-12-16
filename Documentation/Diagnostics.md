# Application Insights and Diagnostics

When developing and maintaining an application, especially one triggered by service bus messages, gaining insights into its operations is crucial. This documentation outlines key resources for tracking application behavior and identifying issues.

## Slow system or too much pressure on the database.

See configuration for options to turn up or down for number of concurrent messages that can be run side-by-side, concurrent tasks per function and sizing of jobs and memory use.

Scaling up resources and increasing these numbers can speed things up. Reducing the number of consurrent messages or concurrent tasks per function can remove pressure from the database.

## Service Bus Monitoring

For a service bus-triggered application, you can gain immediate insights by monitoring the Service Bus queues:

- **Azure Portal**: Check the Service Bus namespaces for metrics like message count, dead-lettered messages, and transfer message counts to understand throughput and identify potential backlogs or errors.

## Application Insights

Our system utilizes a common Application Insights resource to aggregate logging information from all functions. This provides a centralized view of operational health.

### Locating Logs in Application Insights

To locate logs in Application Insights:

1. Navigate to the Application Insights resource in the Azure portal.
2. Use **Logs** feature.

```
traces | project timestamp, operation_Name, message, operation_Id, severityLevel
| where operation_Name startswith "dataraw-receive"
```

```
exceptions | where operation_Name startswith "dataraw" and tostring(details[0].parsedStack) contains "DIH"
| project timestamp, problemId, severityLevel, operation_Name, operation_Id, outerMessage, innermostMessage, stackTrace = tostring(details[0].parsedStack)
```

```
performanceCounters
| where name == "% Processor Time"
| summarize AvgProcessorTime=avg(value) * 100, MaxProcessorTime=max(value) * 100, MinProcessorTime=min(value) * 100 by bin(timestamp, 1h), cloud_RoleName
| project TimeRange=timestamp, Role=cloud_RoleName, 
          AverageProcessorUtilization=strcat(round(AvgProcessorTime, 2), "%"), 
          PeakProcessorUtilization=strcat(round(MaxProcessorTime, 2), "%"), 
          IdleProcessorUtilization=strcat(round(MinProcessorTime, 2), "%")
| order by TimeRange asc
```



### Identifying Errors and Exceptions

To quickly identify recent errors and exceptions:

1. Go to the **Failures** section to see the overview of exceptions, failed requests, and other anomalies.
2. Use the provided interactive tools to filter and segment the failure data.
3. To delve into specific issues, use the detailed **Exception** blade to drill down into stack traces and related telemetry.

## Live Logging with Microsoft's standard logging

Our applications employ the Microsoft standard logging mechanism with varying levels of verbosity: Debug, Information, Warning, and Error.

### Viewing Live Logs

To view live logs:

1. In the Azure Portal, navigate to your Function App's **Monitoring** section.
2. Select **Logs (preview)** or **Log stream** for real-time logging information.
3. The Log stream displays a live feed of log events. Filter this feed by the desired log level to focus on the relevant events.

Remember that the volume and detail of logged events depend on the log level configuration. Higher verbosity (like Debug logs) may provide more insights but can also incur higher costs and more noise in the data.

---

Properly leveraging these diagnostic tools can provide a wealth of information and significantly aid in the troubleshooting process, ultimately leading to a more stable and performant application.

## Application Logging Tables

The application logs data to various tables, which are invaluable for diagnosing issues. Here’s a prioritized list based on their importance:

### 1. `RawDataActiveBatches`
This table tracks active batches. It's crucial for understanding what's currently processing. If a Data Object Type Name appears here, it's locked for the batch ID mentioned. These locks release upon batch completion. However, if there's a failure, manual intervention may be needed to remove the lock. Otherwise, locks expire after a duration (default or specified by `DIH:Functions:BatchTimeoutSeconds` in configuration).

```plaintext
Note: Persistent locks may indicate a problem that requires investigation.
```

### 2. `RawDataImportLog`
Logs all import operations and serves as the go-to for historical data on past runs. 

### 3. `RawDataCanceledBatches`
Keeps a record of canceled batches. A batch listed here won't run again, so this is important for understanding potentially problematic batches or manual cancellations.

### 4. `RawDataUnchangedIds` & `RawDataDeletedIds`
Both tables store IDs that remain unchanged or are deleted during processing. They are essential for auditing changes but are less critical as they are purged post-batch.

### 5. `RawDataBatchesHandled`
Tracks completed batches. It's purged post-completion, so it’s less critical for long-term insights but useful for recent activity.







