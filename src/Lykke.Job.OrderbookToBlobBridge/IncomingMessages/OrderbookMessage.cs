using System;
using System.Collections.Generic;

namespace Lykke.Job.OrderbookToBlobBridge.IncomingMessages
{
    public class OrderbookMessage
    {
        public string Source { get; set; }

        public string Asset { get; set; }

        public DateTime Timestamp { get; set; }

        public List<VolumePrice> Asks { get; set; }

        public List<VolumePrice> Bids { get; set; }
    }

    public class VolumePrice
    {
        public decimal Volume { get; set; }

        public decimal Price { get; set; }
    }
}
