using System;
using System.Text;
using System.IO;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Common.Log;

namespace Lykke.Job.OrderbookToBlobBridge.AzureRepositories
{
    public class BlobSaver
    {
        private const string _timeFormat = "yyyy-MM-dd-HH";
        private const int _warningQueueCount = 1000;
        private const int _maxBlockSize = 4 * 1024 * 1024; // 4 Mb
        private readonly ILog _log;
        private readonly string _containerName;
        private readonly CloudBlobClient _blobClient;
        private readonly List<Tuple<DateTime, string>> _queue = new List<Tuple<DateTime, string>>();
        private readonly SemaphoreSlim _lock = new SemaphoreSlim(1, 1);
        private readonly int _maxInBatch;
        private readonly int _minBatchCount;
        private readonly TimeSpan _delay = TimeSpan.FromMilliseconds(100);
        private readonly BlobRequestOptions _blobRequestOptions = new BlobRequestOptions
        {
            MaximumExecutionTime = TimeSpan.FromMinutes(10),
        };

        private DateTime? _lastDay;
        private DateTime _lastWarning = DateTime.MinValue;
        private volatile bool _mustStop;
        private Thread _thread;
        private CloudAppendBlob _blob;
        private CloudAppendBlob _restartBlob;

        public const string RestartContainer = "orderbook-to-blob-bridge-restart";

        public BlobSaver(
            CloudStorageAccount storageAccount,
            string containerName,
            int maxBatchCount,
            int minBatchCount,
            ILog log)
        {
            _containerName = containerName.Replace('.', '-').ToLower();
            _maxInBatch = maxBatchCount > 0 ? maxBatchCount : 1000;
            _minBatchCount = minBatchCount > 0 ? minBatchCount : 10;
            _log = log;
            _blobClient = storageAccount.CreateCloudBlobClient();

            _thread = new Thread(ProcessQueue) { Name = "OrderbookToBlobBridgeLoop" };
            _thread.Start();
        }

        public async Task SaveDataItemAsync(string item)
        {
            int count;
            await _lock.WaitAsync();
            try
            {
                _queue.Add(new Tuple<DateTime, string>(DateTime.UtcNow, item));
                count = _queue.Count;
            }
            finally
            {
                _lock.Release();
            }
            if (count <= _warningQueueCount)
                return;

            var now = DateTime.UtcNow;
            if (now.Subtract(_lastWarning) >= TimeSpan.FromMinutes(1))
            {
                _lastWarning = now;
                await _log.WriteWarningAsync(
                    "BlobSaver.SaveDataItemAsync",
                    _containerName,
                    $"{count} items in saving queue (> {_warningQueueCount}) - thread status: {(_thread != null ? _thread.IsAlive.ToString() : "missing")}");
            }
        }

        public void Stop()
        {
            _mustStop = true;
            if (_restartBlob != null)
            {
                try
                {
                    SaveToBlobAsync(_restartBlob, _queue.Count, true).Wait();
                }
                catch (Exception ex)
                {
                    _log.WriteErrorAsync(
                        "BlobSaver.Stop." + _containerName,
                        _restartBlob?.Uri != null ? _restartBlob.Uri.ToString() : "",
                        ex).Wait();
                }
            }
        }

        private async void ProcessQueue(object state)
        {
            while (true)
            {
                try
                {
                    _restartBlob = await GetWriteBlobAsync(RestartContainer, _containerName);
                    string allData = await _restartBlob.DownloadTextAsync();
                    if (!string.IsNullOrWhiteSpace(allData))
                    {
                        var items = allData.Split(Environment.NewLine);
                        var list = new List<Tuple<DateTime, string>>();
                        for (int i = 0; i < items.Length; i += 2)
                        {
                            bool dateParsed = DateTime.TryParseExact(
                                items[i].Trim(),
                                _timeFormat,
                                CultureInfo.InvariantCulture,
                                DateTimeStyles.AssumeUniversal,
                                out DateTime date);
                            if (!dateParsed)
                                continue;
                            list.Add(new Tuple<DateTime, string>(date, items[i + 1]));
                        }
                        _queue.InsertRange(0, list);

                        await _log.WriteInfoAsync("BlobSaver.ProcessQueue", _containerName, $"Loaded {list.Count} items");
                    }

                    var containerRef = _blobClient.GetContainerReference(_containerName);
                    if (!(await containerRef.ExistsAsync()))
                    {
                        await containerRef.CreateAsync(BlobContainerPublicAccessType.Off, null, null);
                        await _log.WriteInfoAsync("BlobSaver.ProcessQueue", _containerName, "Container was created");
                    }
                    break;
                }
                catch (Exception ex)
                {
                    await _log.WriteErrorAsync("BlobSaver.ProcessQueue", _containerName, ex);
                }
            }

            await _log.WriteInfoAsync("BlobSaver.ProcessQueue", _containerName, "Processing started");

            while (true)
            {
                try
                {
                    await ProcessQueueAsync();
                }
                catch (Exception ex)
                {
                    await _log.WriteErrorAsync(
                        "BlobSaver.ProcessQueue",
                        _blob?.Uri != null ? _blob.Uri.ToString() : "",
                        ex);
                }
            }
        }

        private async Task ProcessQueueAsync()
        {
            int itemsCount = _queue.Count;
            if (_queue.Count > _warningQueueCount)
                await _log.WriteInfoAsync(
                    "BlobSaver.ProcessQueueAsync." + _containerName,
                    _blob?.Uri != null ? _blob.Uri.ToString() : "",
                    $"{itemsCount} items in queue");
            if (itemsCount == 0 || itemsCount < _minBatchCount && _lastDay.HasValue && DateTime.UtcNow.Subtract(_lastDay.Value) < TimeSpan.FromHours(1))
            {
                if (!_mustStop)
                    await Task.Delay(_delay);
                return;
            }

            if (_queue.Count > _warningQueueCount)
                await _log.WriteInfoAsync("BlobSaver.ProcessQueueAsync", _containerName, $"BeforeCount - {itemsCount}");

            Tuple<DateTime, string> pair;
            int count = 0;
            while (count < _maxInBatch && count < itemsCount)
            {
                pair = _queue[count];
                if (!_lastDay.HasValue)
                    _lastDay = pair.Item1;
                if (pair.Item1.Date != _lastDay.Value.Date || pair.Item1.Hour != _lastDay.Value.Hour)
                {
                    if (count == 0)
                    {
                        _lastDay = pair.Item1;
                        _blob = null;
                    }
                    else
                    {
                        break;
                    }
                }
                ++count;
            }

            if (_queue.Count > _warningQueueCount)
                await _log.WriteInfoAsync("BlobSaver.ProcessQueueAsync", _containerName, $"AfterCount - {count}");

            if (count == 0)
                return;

            await SaveQueueAsync(count);
        }

        private async Task SaveQueueAsync(int count)
        {
            if (_queue.Count > _warningQueueCount)
                await _log.WriteInfoAsync("BlobSaver.SaveQueueAsync", _containerName, $"BeforeCount - {count}");

            int i;
            int allLength = 0;
            for (i = 0; i < count; ++i)
            {
                allLength += 2 + _queue[i].Item2.Length;
                if (allLength > _maxBlockSize)
                    break;
            }

            if (_queue.Count > _warningQueueCount)
                await _log.WriteInfoAsync("BlobSaver.SaveQueueAsync", _containerName, $"AfterCount - {i}");

            if (i == 0)
            {
                await _log.WriteErrorAsync(
                    "BlobSaver.SaveQueueAsync." + _containerName,
                    _queue[0].Item2,
                    new InvalidOperationException("Could not append new block. Item is too large!"));
                await _lock.WaitAsync();
                try
                {
                    _queue.RemoveAt(0);
                }
                finally
                {
                    _lock.Release();
                }
                return;
            }

            if (_blob == null)
            {
                string blobKey = _queue[0].Item1.ToString(_timeFormat);
                _blob = await GetWriteBlobAsync(_containerName, blobKey);

                if (_queue.Count > _warningQueueCount)
                    await _log.WriteInfoAsync(
                        "BlobSaver.SaveQueueAsync." + _containerName,
                        _blob?.Uri != null ? _blob.Uri.ToString() : "",
                        "Blob was recreated");
            }

            await SaveToBlobAsync(_blob, i, false);
        }

        private async Task SaveToBlobAsync(CloudAppendBlob blob, int count, bool withDates)
        {
            using (var stream = new MemoryStream())
            {
                using (var writer = new StreamWriter(stream))
                {
                    for (int j = 0; j < count; j++)
                    {
                        if (withDates)
                            writer.WriteLine(_queue[j].Item1.ToString(_timeFormat));
                        writer.WriteLine(_queue[j].Item2);
                    }
                    writer.Flush();
                    stream.Position = 0;
                    await blob.AppendBlockAsync(stream, null, null, _blobRequestOptions, null);
                }
            }

            if (_queue.Count > _warningQueueCount)
                await _log.WriteInfoAsync(
                    "BlobSaver.SaveToBlobAsync." + _containerName,
                    blob?.Uri != null ? blob.Uri.ToString() : "",
                    "Items were saved to blob");

            bool isLocked = await _lock.WaitAsync(TimeSpan.FromSeconds(1));
            if (isLocked)
            {
                try
                {
                    _queue.RemoveRange(0, count);
                }
                finally
                {
                    _lock.Release();
                }
            }
            else
            {
                await _log.WriteWarningAsync(
                    "BlobSaver.SaveToBlobAsync",
                    _containerName,
                    "Using unsafe queue clearing");
                _queue.RemoveRange(0, count);
            }

            if (_queue.Count > _warningQueueCount)
                await _log.WriteInfoAsync(
                    "BlobSaver.SaveToBlobAsync." + _containerName,
                    blob?.Uri != null ? blob.Uri.ToString() : "",
                    "Saved items were removed from queue");
        }

        private async Task<CloudAppendBlob> GetWriteBlobAsync(string containerName, string storagePath)
        {
            var blobContainer = _blobClient.GetContainerReference(containerName);
            var blob = blobContainer.GetAppendBlobReference(storagePath);
            if (!(await blob.ExistsAsync()))
            {
                try
                {
                    await blob.CreateOrReplaceAsync(AccessCondition.GenerateIfNotExistsCondition(), null, null);
                    blob.Properties.ContentType = "text/plain";
                    blob.Properties.ContentEncoding = Encoding.UTF8.WebName;
                    await blob.SetPropertiesAsync(null, _blobRequestOptions, null);
                }
                catch (StorageException)
                {
                }
            }
            return blob;
        }
    }
}
