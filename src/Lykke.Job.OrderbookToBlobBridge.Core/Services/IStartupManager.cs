using System.Threading.Tasks;

namespace Lykke.Job.OrderbookToBlobBridge.Core.Services
{
    public interface IStartupManager
    {
        Task StartAsync();
    }
}