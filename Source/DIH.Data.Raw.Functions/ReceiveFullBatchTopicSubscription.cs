using DIH.Common;
using DIH.Common.Services.Messaging;
using DIH.Data.Raw.Functions.Helpers;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Configuration;
using Newtonsoft.Json;
using System;
using System.Threading.Tasks;

namespace DIH.Data.Raw.Functions
{
    public class ReceiveFullBatchTopicSubscription
    {
        private IMessagingService DataRawMessageService { get; set; }
        protected string ReceiveFullbatchQueueName { get; set; }

        private const string topicName = "ingestion-fullbatch";
        private const string subscriptionName = "ingestion-fullbatch-dataraw-subscription";

        public ReceiveFullBatchTopicSubscription(IConfiguration configuration, IMessagingServiceDictionary messageServiceDictionary)
        {
            DataRawMessageService = messageServiceDictionary.Get(Layer.DataRaw);
            ReceiveFullbatchQueueName = configuration[ConfigKeys.Data_Raw_ReceiveFullbatch_QueueName] ?? throw new InvalidOperationException($"Missing config {ConfigKeys.Data_Raw_ReceiveFullbatch_QueueName}");

        }

        [FunctionName("dataraw-receive-fullbatch-topic-subscriber")]
        public async Task Run([ServiceBusTrigger(topicName, subscriptionName, Connection = MessageHandlerHelper.ServiceBusConnectionString, AutoCompleteMessages = true)] string messageText)
        {
            var message = JsonConvert.DeserializeObject<IngestionFullBatchFileMessage>(messageText);
            if (message == null) throw new InvalidOperationException($"Error deserializing message as {nameof(IngestionFullBatchFileMessage)}. Message: {messageText}");

            await DataRawMessageService.EnqueueMessageAsync(ReceiveFullbatchQueueName, message);
        }
    }
}

