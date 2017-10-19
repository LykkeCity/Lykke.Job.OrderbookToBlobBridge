using Autofac;
using Common;
using Common.Log;
using Lykke.SettingsReader;
using Lykke.Job.OrderbookToBlobBridge.Core.Services;
using Lykke.Job.OrderbookToBlobBridge.Services;
using Lykke.Job.OrderbookToBlobBridge.Core.Settings;
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

            builder.RegisterType<HealthService>()
                .As<IHealthService>()
                .SingleInstance();

            builder.RegisterType<StartupManager>()
                .As<IStartupManager>();

            builder.RegisterType<ShutdownManager>()
                .As<IShutdownManager>();
            RegisterRabbitMqSubscribers(builder);
        }

        private void RegisterRabbitMqSubscribers(ContainerBuilder builder)
        {
            foreach (var orderbookStream in _settings.OrderbookStreams)
            {
                var subscriber = new OrderbookSubscriber(
                    orderbookStream.RabbitMqConnectionString,
                    orderbookStream.ExchangeName,
                    _settings.BatchCount,
                    orderbookStream.OutputBlobConnectionString,
                    _console,
                    _log);
                builder.RegisterInstance(subscriber)
                    .As<IStartable>()
                    .As<IStopable>();
            }
        }
    }
}
