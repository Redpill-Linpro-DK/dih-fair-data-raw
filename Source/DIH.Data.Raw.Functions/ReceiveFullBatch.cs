using DIH.Data.Raw.Functions.Helpers;
using Microsoft.Azure.WebJobs;
using System.Threading.Tasks;

namespace DIH.Data.Raw.Functions
{
    public class ReceiveFullBatch
    {
        private MessageHandlerHelper MessageHandlerHelper { get; set; }
        private const string queueName = "dataraw-receive-fullbatch";

        public ReceiveFullBatch(MessageHandlerHelper messageHandlerManager)
        {
            MessageHandlerHelper = messageHandlerManager;
        }

        [FunctionName("dataraw-receive-fullbatch")]
        public async Task Run([ServiceBusTrigger(queueName, Connection = MessageHandlerHelper.ServiceBusConnectionString, AutoCompleteMessages = true)] string messageText)
        {
            await MessageHandlerHelper.DispatchMessage(queueName, messageText);
        }
    }
}

