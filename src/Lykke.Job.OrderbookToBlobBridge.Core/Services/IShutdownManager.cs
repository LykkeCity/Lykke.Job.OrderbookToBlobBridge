using System.Threading.Tasks;
using Common;

namespace Lykke.Job.OrderbookToBlobBridge.Core.Services
{
    public interface IShutdownManager
    {
        Task StopAsync();

        void Add(IStopable stopItem);
    }
}
