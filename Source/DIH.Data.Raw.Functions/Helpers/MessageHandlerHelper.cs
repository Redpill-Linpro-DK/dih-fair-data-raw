using DIH.Data.Raw.MessageHandlers;
using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace DIH.Data.Raw.Functions.Helpers
{
    public class MessageHandlerHelper
    {
        public const string ServiceBusConnectionString = "Data:Raw:ServiceBus";

        private IEnumerable<IMessageHandler> MessageHandlers { get; set; }
        private IConfiguration Configuration { get; set; }

        public MessageHandlerHelper(
            IConfiguration configuration,
            IEnumerable<IMessageHandler> messageHandlers)
        {
            Configuration = configuration;
            MessageHandlers = messageHandlers;
        }

        public async Task DispatchMessage(string queueName, string messageText)
        {
            string mappedQueueName = queueName.StartsWith("%") && queueName.EndsWith("%") ? Configuration[queueName.Replace("%", "")] : queueName;

            if (string.IsNullOrEmpty(mappedQueueName)) throw new ArgumentException($"Invalid queue name {queueName}", nameof(queueName));

            IEnumerable<IMessageHandler> handlers = MessageHandlers.Where(handler => handler.CanHandleQueue(mappedQueueName));

            foreach (var handler in handlers)
            {
                await handler.Run(mappedQueueName, messageText);
            }
        }
    }
}

