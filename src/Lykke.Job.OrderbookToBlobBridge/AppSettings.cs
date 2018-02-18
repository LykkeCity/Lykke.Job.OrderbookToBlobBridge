using Lykke.SettingsReader.Attributes;

namespace Lykke.Job.OrderbookToBlobBridge
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

    public class OrderbookToBlobBridgeSettings
    {
        [AzureTableCheck]
        public string LogsConnectionString { get; set; }

        public string LogsTableName { get; set; }

        [AmqpCheck]
        public string RabbitMqConnectionString { get; set; }

        public string ExchangeName { get; set; }

        [AzureBlobCheck]
        public string OutputBlobConnectionString { get; set; }

        public int MaxBatchCount { get; set; }

        public int MinBatchCount { get; set; }
        
        [Optional]
        public bool UseMessagePack { get; set; }
    }
}
