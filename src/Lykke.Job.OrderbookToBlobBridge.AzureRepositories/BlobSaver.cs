using System;
using System.Text;
using System.IO;
using System.Collections.Generic;
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
        private readonly CloudStorageAccount _storageAccount;
        private readonly List<Tuple<DateTime, string>> _queue = new List<Tuple<DateTime, string>>();
        private readonly SemaphoreSlim _lock = new SemaphoreSlim(1, 1);
        private readonly int _maxInBatch;
        private readonly int _minBatchCount;
        private readonly TimeSpan _delay = TimeSpan.FromMilliseconds(100);
        private DateTime _lastDay = DateTime.MinValue;
        private DateTime _lastWarning = DateTime.MinValue;
        private volatile bool _mustStop;
        private CloudAppendBlob _blob;

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
            _storageAccount = storageAccount;

            ThreadPool.QueueUserWorkItem(ProcessQueue);
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
                    $"{count} items in saving queue (> {_warningQueueCount})!");
            }
        }

        public void Stop()
        {
            _mustStop = true;
            while (_queue.Count > 0)
                Thread.Sleep(1000);
        }

        private async void ProcessQueue(object state)
        {
            await _log.WriteInfoAsync("BlobSaver.ProcessQueue", _containerName, "Processing started");

            while (true)
            {
                try
                {
                    var containerRef = GetContainerReference();
                    if (!(await containerRef.ExistsAsync()))
                        await containerRef.CreateAsync(BlobContainerPublicAccessType.Off, null, null);
                    break;
                }
                catch (Exception ex)
                {
                    await _log.WriteErrorAsync("BlobSaver.ProcessQueue", _containerName, ex);
                }
            }

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
            if (itemsCount == 0)
            {
                if (!_mustStop)
                    await Task.Delay(_delay);
                return;
            }

            Tuple<DateTime, string> pair;
            int count = 0;
            while (count <= _maxInBatch && count < itemsCount)
            {
                pair = _queue[count];
                if (pair.Item1.Hour != _lastDay.Hour)
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

            if (count == 0)
                return;

            if (!_mustStop && count == itemsCount && count < _minBatchCount)
                await Task.Delay(_delay);

            if (_blob == null)
            {
                string blobKey = _queue[0].Item1.ToString(_timeFormat);
                _blob = await GetWriteBlobAsync(blobKey);
            }

            await SaveQueueAsync(count);
        }

        private async Task SaveQueueAsync(int count)
        {
            int i;
            int allLength = 0;
            for (i = 0; i < count; ++i)
            {
                allLength += 2 + _queue[i].Item2.Length;
                if (allLength > _maxBlockSize)
                    break;
            }

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

            using (var stream = new MemoryStream())
            {
                using (var writer = new StreamWriter(stream))
                {
                    for (int j = 0; j < i; j++)
                    {
                        writer.WriteLine(_queue[j].Item2);
                    }
                    writer.Flush();
                    stream.Position = 0;
                    await _blob.AppendBlockAsync(stream);
                }
            }

            await _lock.WaitAsync();
            try
            {
                _queue.RemoveRange(0, i);
            }
            finally
            {
                _lock.Release();
            }
        }

        private CloudBlobContainer GetContainerReference()
        {
            var blobClient = _storageAccount.CreateCloudBlobClient();
            return blobClient.GetContainerReference(_containerName);
        }

        private async Task<CloudAppendBlob> GetWriteBlobAsync(string storagePath)
        {
            var blobContainer = GetContainerReference();
            var blob = blobContainer.GetAppendBlobReference(storagePath);
            if (!(await blob.ExistsAsync()))
            {
                try
                {
                    await blob.CreateOrReplaceAsync(AccessCondition.GenerateIfNotExistsCondition(), null, null);
                    blob.Properties.ContentType = "text/plain";
                    blob.Properties.ContentEncoding = Encoding.UTF8.WebName;
                    await blob.SetPropertiesAsync();
                }
                catch (StorageException)
                {
                }
            }
            return blob;
        }
    }
}
