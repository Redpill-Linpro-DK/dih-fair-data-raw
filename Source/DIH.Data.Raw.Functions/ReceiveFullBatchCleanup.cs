using DIH.Data.Raw.Functions.Helpers;
using Microsoft.Azure.WebJobs;
using System.Threading.Tasks;

namespace DIH.Data.Raw.Functions
{
    public class ReceiveFullBatchCleanup
    {
        private MessageHandlerHelper MessageHandlerHelper { get; set; }
        private const string queueName = "dataraw-receive-fullbatch-cleanup";

        public ReceiveFullBatchCleanup(MessageHandlerHelper messageHandlerManager)
        {
            MessageHandlerHelper = messageHandlerManager;
        }

        [Singleton] // only allow 1 instance of this to run at any given time ... 
        [FunctionName("dataraw-receive-fullbatch-cleanup")]
        public async Task Run([ServiceBusTrigger(queueName, Connection = MessageHandlerHelper.ServiceBusConnectionString, AutoCompleteMessages = true)] string messageText)
        {
            await MessageHandlerHelper.DispatchMessage(queueName, messageText);
        }
    }
}

