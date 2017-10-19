using System;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Autofac;
using Autofac.Extensions.DependencyInjection;
using AzureStorage.Tables;
using Common.Log;
using Lykke.Common.ApiLibrary.Middleware;
using Lykke.Common.ApiLibrary.Swagger;
using Lykke.Logs;
using Lykke.SettingsReader;
using Lykke.SlackNotification.AzureQueue;
using Lykke.Job.OrderbookToBlobBridge.Core.Services;
using Lykke.Job.OrderbookToBlobBridge.Core.Settings;
using Lykke.Job.OrderbookToBlobBridge.Models;
using Lykke.Job.OrderbookToBlobBridge.Modules;

namespace Lykke.Job.OrderbookToBlobBridge
{
    public class Startup
    {
        private ILog _log;
        private LogToConsole _consoleLog;

        public IContainer ApplicationContainer { get; private set; }
        public IConfigurationRoot Configuration { get; }

        public Startup(IHostingEnvironment env)
        {
            var builder = new ConfigurationBuilder()
                .SetBasePath(env.ContentRootPath)
                .AddJsonFile("appsettings.json")
                .AddJsonFile($"appsettings.{env.EnvironmentName}.json", true)
                .AddEnvironmentVariables();

            Configuration = builder.Build();
        }

        public IServiceProvider ConfigureServices(IServiceCollection services)
        {
            try
            {
                services.AddMvc()
                    .AddJsonOptions(options =>
                    {
                        options.SerializerSettings.ContractResolver =
                            new Newtonsoft.Json.Serialization.DefaultContractResolver();
                    });

                services.AddSwaggerGen(options =>
                {
                    options.DefaultLykkeConfiguration("v1", "OrderbookToBlobBridge API");
                });

                var builder = new ContainerBuilder();
                var appSettings = Configuration.LoadSettings<AppSettings>();

                _log = CreateLogWithSlack(services, appSettings);

                builder.RegisterModule(
                    new JobModule(
                        appSettings.CurrentValue.OrderbookToBlobBridgeJob,
                        appSettings.Nested(x => x.OrderbookToBlobBridgeJob),
                        _consoleLog,
                        _log));

                builder.Populate(services);

                ApplicationContainer = builder.Build();

                return new AutofacServiceProvider(ApplicationContainer);
            }
            catch (Exception ex)
            {
                _log?.WriteFatalErrorAsync(nameof(Startup), nameof(ConfigureServices), "", ex).Wait();
                throw;
            }
        }

        public void Configure(IApplicationBuilder app, IHostingEnvironment env, IApplicationLifetime appLifetime)
        {
            try
            {
                if (env.IsDevelopment())
                    app.UseDeveloperExceptionPage();

                app.UseLykkeMiddleware("OrderbookToBlobBridge", ex => new ErrorResponse {ErrorMessage = "Technical problem"});

                app.UseMvc();
                app.UseSwagger();
                app.UseSwaggerUi();
                app.UseStaticFiles();

                appLifetime.ApplicationStarted.Register(() => StartApplication().Wait());
                appLifetime.ApplicationStopping.Register(() => StopApplication().Wait());
                appLifetime.ApplicationStopped.Register(() => CleanUp().Wait());
            }
            catch (Exception ex)
            {
                _log?.WriteFatalErrorAsync(nameof(Startup), nameof(ConfigureServices), "", ex).Wait();
                throw;
            }
        }

        private async Task StartApplication()
        {
            try
            {
                // NOTE: Job not yet recieve and process IsAlive requests here

                await ApplicationContainer.Resolve<IStartupManager>().StartAsync();
                await _log.WriteMonitorAsync("", "", "Started");
            }
            catch (Exception ex)
            {
                await _log.WriteFatalErrorAsync(nameof(Startup), nameof(StartApplication), "", ex);
                throw;
            }
        }

        private async Task StopApplication()
        {
            try
            {
                // NOTE: Job still can recieve and process IsAlive requests here, so take care about it if you add logic here.

                await ApplicationContainer.Resolve<IShutdownManager>().StopAsync();
            }
            catch (Exception ex)
            {
                if (_log != null)
                {
                    await _log.WriteFatalErrorAsync(nameof(Startup), nameof(StopApplication), "", ex);
                }
                throw;
            }
        }

        private async Task CleanUp()
        {
            try
            {
                // NOTE: Job can't recieve and process IsAlive requests here, so you can destroy all resources
                
                if (_log != null)
                {
                    await _log.WriteMonitorAsync("", "", "Terminating");
                }
                
                ApplicationContainer.Dispose();
            }
            catch (Exception ex)
            {
                if (_log != null)
                {
                    await _log.WriteFatalErrorAsync(nameof(Startup), nameof(CleanUp), "", ex);
                    (_log as IDisposable)?.Dispose();
                }
                throw;
            }
        }

        private ILog CreateLogWithSlack(IServiceCollection services, IReloadingManager<AppSettings> settings)
        {
            _consoleLog = new LogToConsole();
            var aggregateLogger = new AggregateLogger();

            aggregateLogger.AddLog(_consoleLog);

            // Creating slack notification service, which logs own azure queue processing messages to aggregate log
            var slackService = services.UseSlackNotificationsSenderViaAzureQueue(new AzureQueueIntegration.AzureQueueSettings
            {
                ConnectionString = settings.CurrentValue.SlackNotifications.AzureQueue.ConnectionString,
                QueueName = settings.CurrentValue.SlackNotifications.AzureQueue.QueueName
            }, aggregateLogger);

            var dbLogConnectionStringManager = settings.Nested(x => x.OrderbookToBlobBridgeJob.LogsConnectionString);
            var dbLogConnectionString = dbLogConnectionStringManager.CurrentValue;

            // Creating azure storage logger, which logs own messages to concole log
            if (!string.IsNullOrEmpty(dbLogConnectionString) && !(dbLogConnectionString.StartsWith("${") && dbLogConnectionString.EndsWith("}")))
            {
                var persistenceManager = new LykkeLogToAzureStoragePersistenceManager(
                    AzureTableStorage<LogEntity>.Create(dbLogConnectionStringManager, "OrderbookToBlobBridgeLogs", _consoleLog),
                    _consoleLog);

                var slackNotificationsManager = new LykkeLogToAzureSlackNotificationsManager(slackService, _consoleLog);

                var azureStorageLogger = new LykkeLogToAzureStorage(
                    persistenceManager,
                    slackNotificationsManager,
                    _consoleLog);

                azureStorageLogger.Start();

                aggregateLogger.AddLog(azureStorageLogger);
            }

            return aggregateLogger;
        }
    }
}
