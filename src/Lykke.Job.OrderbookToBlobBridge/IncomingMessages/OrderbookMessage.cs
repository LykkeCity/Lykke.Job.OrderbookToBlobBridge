using System;
using System.Collections.Generic;
using MessagePack;

namespace Lykke.Job.OrderbookToBlobBridge.IncomingMessages
{
    [MessagePackObject]
    public class OrderbookMessage
    {
        [Key(0)]
        public string Source { get; set; }

        [Key(1)]
        public string Asset { get; set; }

        [Key(2)]
        public DateTime Timestamp { get; set; }

        [Key(3)]
        public List<VolumePrice> Asks { get; set; }

        [Key(4)]
        public List<VolumePrice> Bids { get; set; }
    }

    [MessagePackObject]
    public class VolumePrice
    {
        [Key(0)]
        public decimal Volume { get; set; }

        [Key(1)]
        public decimal Price { get; set; }
    }
}
