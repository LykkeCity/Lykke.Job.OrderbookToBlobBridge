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
                .As<IStartupManager>()
                .SingleInstance();

            var shutdownManager = new ShutdownManager(_log);
            builder.RegisterInstance(shutdownManager)
                .As<IShutdownManager>()
                .SingleInstance();

            var subscriber = new OrderbookSubscriber(
                _settings.RabbitMqConnectionString,
                _settings.ExchangeName,
                _settings.MaxBatchCount,
                _settings.MinBatchCount,
                _settings.OutputBlobConnectionString,
                _console,
                _log);
            builder.RegisterInstance(subscriber)
                .As<IStartable>()
                .As<IStopable>();
            shutdownManager.Add(subscriber);
        }
    }
}
