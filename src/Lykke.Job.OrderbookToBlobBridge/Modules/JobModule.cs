using Autofac;
using Common.Log;
using Lykke.Common;
using Lykke.SettingsReader;
using Lykke.Job.OrderbookToBlobBridge.Core.Services;
using Lykke.Job.OrderbookToBlobBridge.Services;
using Lykke.Job.OrderbookToBlobBridge.RabbitSubscribers;

namespace Lykke.Job.OrderbookToBlobBridge.Modules
{
    public class JobModule : Module
    {
        private readonly OrderbookToBlobBridgeSettings _settings;
        private readonly IReloadingManager<OrderbookToBlobBridgeSettings> _settingsManager;
        private readonly ILog _log;
        private readonly IConsole _console;

        public JobModule(
            OrderbookToBlobBridgeSettings settings,
            IReloadingManager<OrderbookToBlobBridgeSettings> settingsManager,
            IConsole console,
            ILog log)
        {
            _settings = settings;
            _log = log;
            _console = console;
            _settingsManager = settingsManager;
        }

        protected override void Load(ContainerBuilder builder)
        {
            builder.RegisterInstance(_log)
                .As<ILog>()
                .SingleInstance();

            builder.RegisterInstance(_console)
                .As<IConsole>()
                .SingleInstance();

            builder.RegisterType<HealthService>()
                .As<IHealthService>()
                .SingleInstance();

            builder.RegisterType<StartupManager>()
                .As<IStartupManager>()
                .SingleInstance();

            builder.RegisterType<ShutdownManager>()
                .As<IShutdownManager>()
                .SingleInstance();

            builder.RegisterResourcesMonitoring(_log);

            builder.RegisterType<OrderbookSubscriber>()
                .As<IStartable>()
                .WithParameter("rabbitMqConnectionString", _settings.RabbitMqConnectionString)
                .WithParameter("exchangeName", _settings.ExchangeName)
                .WithParameter("maxBatchCount", _settings.MaxBatchCount)
                .WithParameter("minBatchCount", _settings.MinBatchCount)
                .WithParameter("blobStorageConnectionString", _settings.OutputBlobConnectionString)
                .WithParameter("useMessagePack", _settings.UseMessagePack);
        }
    }
}
