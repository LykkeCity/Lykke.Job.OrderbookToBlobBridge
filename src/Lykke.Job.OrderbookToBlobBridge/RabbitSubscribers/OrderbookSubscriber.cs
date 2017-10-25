using System;
using System.Text;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Autofac;
using Common;
using Common.Log;
using Lykke.RabbitMqBroker;
using Lykke.RabbitMqBroker.Subscriber;
using Lykke.Job.OrderbookToBlobBridge.AzureRepositories;
using Lykke.Job.OrderbookToBlobBridge.IncomingMessages;

namespace Lykke.Job.OrderbookToBlobBridge.RabbitSubscribers
{
    public class OrderbookSubscriber : IStartable, IStopable
    {
        private readonly string _rabbitMqConnectionString;
        private readonly string _exchangeName;
        private readonly int _batchCount;
        private readonly CloudStorageAccount _storageAccount;
        private readonly IConsole _console;
        private readonly ILog _log;

        private RabbitMqSubscriber<OrderbookMessage> _subscriber;

        private readonly ConcurrentDictionary<string, BlobSaver> _dict = new ConcurrentDictionary<string, BlobSaver>();

        public OrderbookSubscriber(
            string rabbitMqConnectionString,
            string exchangeName,
            int batchCount,
            string blobStorageConnectionString,
            IConsole console,
            ILog log)
        {
            _rabbitMqConnectionString = rabbitMqConnectionString;
            _exchangeName = exchangeName;
            _batchCount = batchCount;
            _storageAccount = CloudStorageAccount.Parse(blobStorageConnectionString);
            _console = console;
            _log = log;
        }

        public void Start()
        {
            // NOTE: Read https://github.com/LykkeCity/Lykke.RabbitMqDotNetBroker/blob/master/README.md to learn
            // about RabbitMq subscriber configuration

            var settings = RabbitMqSubscriptionSettings
                .CreateForSubscriber(_rabbitMqConnectionString, _exchangeName, "orderbooktoblobbridge")
                .MakeDurable();

            _subscriber = new RabbitMqSubscriber<OrderbookMessage>(settings,
                    new ResilientErrorHandlingStrategy(_log, settings,
                        retryTimeout: TimeSpan.FromSeconds(10),
                        next: new DeadQueueErrorHandlingStrategy(_log, settings)))
                .SetMessageDeserializer(new JsonMessageDeserializer<OrderbookMessage>())
                .SetMessageReadStrategy(new MessageReadWithTemporaryQueueStrategy())
                .Subscribe(ProcessMessageAsync)
                .CreateDefaultBinding()
                .SetConsole(_console)
                .SetLogger(_log)
                .Start();
        }

        public void Dispose()
        {
            _subscriber?.Dispose();
        }

        public void Stop()
        {
            _subscriber.Stop();
        }

        private async Task ProcessMessageAsync(OrderbookMessage item)
        {
            try
            {
                if (item.Asks != null && item.Asks.Count > 0)
                    await ProcessAsync(item, true);
                if (item.Bids != null && item.Bids.Count > 0)
                    await ProcessAsync(item, false);
            }
            catch (Exception exc)
            {
                await _log.WriteErrorAsync(
                    nameof(OrderbookSubscriber),
                    nameof(ProcessMessageAsync),
                    exc);
            }
        }

        private async Task ProcessAsync(OrderbookMessage item, bool isAsk)
        {
            string containerName = GetContainerName(item, isAsk);
            if (!_dict.ContainsKey(containerName))
                _dict.TryAdd(containerName, new BlobSaver(_storageAccount, containerName, _batchCount, _log));
            string itemStr = GetItemSring(item, isAsk);
            await _dict[containerName].SaveDataItemAsync(itemStr);
        }

        private string GetContainerName(OrderbookMessage item, bool isAsk)
        {
            string source = item.Source == null
                ? ""
                : item.Source.Replace('.', '-');
            string suffix = isAsk ? "ask" : "bid";
            return $"{source}-{item.Asset ?? ""}-{suffix}";
        }

        private string GetItemSring(OrderbookMessage item, bool isAsk)
        {
            var strBuilder = new StringBuilder("{\"t\":\"");
            strBuilder.Append(item.Timestamp.ToString("mm:ss.fff"));
            strBuilder.Append("\",\"p\":[");
            var collection = isAsk ? item.Asks : item.Bids;
            for (int i = 0; i < collection.Count; ++i)
            {
                var price = collection[i];
                if (i > 0)
                    strBuilder.Append(",");
                strBuilder.Append("{\"v\":");
                strBuilder.Append(price.Volume);
                strBuilder.Append(",\"p\":");
                strBuilder.Append(price.Price);
                strBuilder.Append("}");
            }
            strBuilder.Append("]}");

            return strBuilder.ToString();
        }
    }
}
