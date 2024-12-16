namespace DIH.Data.Raw.MessageHandlers
{
    public interface IMessageHandler
    {
        bool CanHandleQueue(string queueName);
        Task Run(string queueName, string queueMessage);
    }
}

