using System.Collections.Generic;
using System.Threading.Tasks;
using Common;
using Common.Log;
using Lykke.Job.OrderbookToBlobBridge.Core.Services;

namespace Lykke.Job.OrderbookToBlobBridge.Services
{
    public class ShutdownManager : IShutdownManager
    {
        private readonly ILog _log;
        private readonly List<IStopable> _stopItems = new List<IStopable>();

        public ShutdownManager(ILog log)
        {
            _log = log;
        }

        public void Add(IStopable stopItem)
        {
            _stopItems.Add(stopItem);
        }

        public async Task StopAsync()
        {
            foreach (var stopItem in _stopItems)
            {
                stopItem.Stop();
            }
        }
    }
}
