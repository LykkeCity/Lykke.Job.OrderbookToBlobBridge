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
        private const int _warningQueueCount = 1000;
        private const int _writeBlockSize = 3 * 1024 * 1024; // 4 Mb - Max
        private readonly ILog _log;
        private readonly string _containerName;
        private readonly CloudStorageAccount _storageAccount;
        private readonly List<Tuple<DateTime, string>> _queue = new List<Tuple<DateTime, string>>();
        private readonly SemaphoreSlim _lock = new SemaphoreSlim(1, 1);
        private readonly int _maxInBatch;
        private DateTime _lastDay = DateTime.MinValue;
        private volatile bool _mustStop;

        public BlobSaver(
            CloudStorageAccount storageAccount,
            string containerName,
            int batchCount,
            ILog log)
        {
            _containerName = containerName.Replace('.', '-').ToLower();
            _maxInBatch = batchCount > 0 ? batchCount : 1000;
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
            if (count > _warningQueueCount)
                await _log.WriteWarningAsync(
                    "BlobSaver.SaveDataItemAsync",
                    _containerName,
                    $"{count} items in saving queue (> {_warningQueueCount})!");
        }

        public void Stop()
        {
            _mustStop = true;
            while (_queue.Count > 0)
                Thread.Sleep(1000);
        }

        private async void ProcessQueue(object state)
        {
            var containerRef = GetContainerReference();
            if (!(await containerRef.ExistsAsync()))
                await containerRef.CreateAsync(BlobContainerPublicAccessType.Off, null, null);

            CloudAppendBlob blob = null;

            while (true)
            {
                int itemsCount = _queue.Count;
                if (itemsCount == 0)
                {
                    if (!_mustStop)
                        await Task.Delay(TimeSpan.FromSeconds(1));
                    continue;
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
                            blob = null;
                        }
                        else
                        {
                            break;
                        }
                    }
                    ++count;
                }

                if (count == 0)
                    continue;

                if (blob == null)
                {
                    try
                    {
                        string blobKey = _queue[0].Item1.ToString("yyyy-MM-dd-HH");
                        blob = await GetWriteBlobAsync(blobKey);
                    }
                    catch (Exception exc)
                    {
                        await _log.WriteErrorAsync("BlobSaver.ProcessQueue", _containerName, exc);
                        continue;
                    }
                }

                await SaveQueueAsync(blob, count);
            }
        }

        private async Task SaveQueueAsync(CloudAppendBlob blob, int count)
        {
            List<Tuple<DateTime, string>> items = null;
            try
            {
                items = _queue.GetRange(0, count);
                await _lock.WaitAsync();
                try
                {
                    _queue.RemoveRange(0, count);
                }
                finally
                {
                    _lock.Release();
                }

                int allLength = 0;
                StringBuilder strBuilder = new StringBuilder();
                int i;
                for (i = 0; i < items.Count; ++i)
                {
                    allLength += items[i].Item2.Length;
                    if (allLength > _writeBlockSize)
                        break;
                    strBuilder.AppendLine(items[i].Item2);
                }
                string text = strBuilder.ToString();
                byte[] bytes = Encoding.UTF8.GetBytes(text);
                var stream = new MemoryStream(bytes);
                await blob.AppendBlockAsync(stream);
                if (i < items.Count)
                {
                    await _lock.WaitAsync();
                    try
                    {
                        var notSaved = items.GetRange(i, items.Count - i);
                        _queue.InsertRange(0, notSaved);
                    }
                    finally
                    {
                        _lock.Release();
                    }
                }
            }
            catch (Exception exc)
            {
                if (items != null)
                {
                    await _lock.WaitAsync();
                    try
                    {
                        _queue.InsertRange(0, items);
                    }
                    finally
                    {
                        _lock.Release();
                    }
                }

                await _log.WriteErrorAsync(
                    "BlobSaver.ProcessQueue",
                    (blob?.Uri != null ? blob.Uri.ToString() : ""),
                    exc);

                await Task.Delay(TimeSpan.FromSeconds(3));
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
