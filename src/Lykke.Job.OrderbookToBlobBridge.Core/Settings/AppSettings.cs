namespace Lykke.Job.OrderbookToBlobBridge.Core.Settings
{
    public class AppSettings
    {
        public OrderbookToBlobBridgeSettings OrderbookToBlobBridgeJob { get; set; }

        public SlackNotificationsSettings SlackNotifications { get; set; }
    }

    public class SlackNotificationsSettings
    {
        public AzureQueuePublicationSettings AzureQueue { get; set; }
    }

    public class AzureQueuePublicationSettings
    {
        public string ConnectionString { get; set; }

        public string QueueName { get; set; }
    }

    public class DbSettings
    {
        public string LogsConnString { get; set; }
    }

    public class OrderbookToBlobBridgeSettings
    {
        public string LogsConnectionString { get; set; }

        public string LogsTableName { get; set; }

        public string RabbitMqConnectionString { get; set; }

        public string ExchangeName { get; set; }

        public string OutputBlobConnectionString { get; set; }

        public int MaxBatchCount { get; set; }

        public int MinBatchCount { get; set; }
    }
}
