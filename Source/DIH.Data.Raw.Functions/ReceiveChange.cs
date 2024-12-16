using DIH.Data.Raw.Functions.Helpers;
using Microsoft.Azure.WebJobs;
using System.Threading.Tasks;

namespace DIH.Data.Raw.Functions
{
    public class ReceiveChange
    {
        private MessageHandlerHelper MessageHandlerHelper { get; set; }
        //private const string queueName = "dataraw-receive-change";
        private const string topicName = "ingestion-change";
        private const string subscriptionName = "ingestion-change-dataraw-subscription";

        public ReceiveChange(MessageHandlerHelper messageHandlerManager)
        {
            MessageHandlerHelper = messageHandlerManager;
        }

        //[FunctionName("dataraw-receive-change")]
        //public async Task Run([ServiceBusTrigger(queueName, Connection = MessageHandlerHelper.ServiceBusConnectionString, AutoCompleteMessages = true)] string messageText)
        //{
        //    await MessageHandlerHelper.DispatchMessage(queueName, messageText);
        //}

        [FunctionName("ingestion-change-dataraw-subscription")]
        public async Task Run([ServiceBusTrigger(topicName, subscriptionName, Connection = MessageHandlerHelper.ServiceBusConnectionString, AutoCompleteMessages = true)] string messageText)
        {
            await MessageHandlerHelper.DispatchMessage(subscriptionName, messageText);
        }

    }
}

