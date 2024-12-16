namespace DIH.Data.Raw.MessageHandlers.Helpers
{
    internal class AbortEntireBatchException : Exception
    {
        internal AbortEntireBatchException(string message) : base(message) { }
    }
}

