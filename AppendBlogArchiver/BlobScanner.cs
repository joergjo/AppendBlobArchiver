using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace AppendBlogArchiver
{
    public class BlobScanner
    {
        private readonly Func<CloudBlob, bool> _scannerStrategy;
        private readonly CloudBlobClient _blobClient;

        public BlobScanner()
        {
            string connectionString = Environment.GetEnvironmentVariable("StorageConnectionString");
            var storageAccount = CloudStorageAccount.Parse(connectionString);
            _blobClient = storageAccount.CreateCloudBlobClient();
            _scannerStrategy = blob =>
            {
                return blob.BlobType == BlobType.AppendBlob;

                // Or rather something like
                //if (blob.BlobType == BlobType.AppendBlob)
                //{
                //    var age = DateTime.UtcNow - blob.Properties.LastModified.Value;
                //    return age.Days > 30;
                //}
                //return false;
            };
        }

        // Once DI is fully supported in Azue Functions, you will be able to inject these dependencies 
        // through constructor inhection
        //public BlobScanner(CloudBlobClient blobClient, Func<CloudBlob, bool> scannerStrategy)
        //{
        //    _blobClient = blobClient ?? throw new ArgumentNullException(nameof(blobClient)); 
        //    _scannerStrategy = scannerStrategy ?? throw new ArgumentNullException(nameof(scannerStrategy));
        //}

        [FunctionName(nameof(BlobScanner))]
        public async Task Run(
            [TimerTrigger("0 */2 * * * *")]TimerInfo timer,
            [Blob("source", FileAccess.Read, Connection = "StorageConnectionString")] CloudBlobContainer container,
            [Queue("archivecommands", Connection = "StorageConnectionString")]IAsyncCollector<BlobArchiveCommand> commands,
            ILogger logger,
            CancellationToken cancellationToken)
        {
            logger.LogInformation($"{nameof(BlobScanner)} function executing at: {DateTime.UtcNow}.");

            var continuationToken = default(BlobContinuationToken);
            int blobsFound = 0;
            int blobsToBeArchived = 0;

            do
            {
                var results = await container.ListBlobsSegmentedAsync(
                    prefix: null,
                    useFlatBlobListing: true,
                    blobListingDetails: BlobListingDetails.None,
                    maxResults: 250,
                    currentToken: continuationToken,
                    options: null,
                    operationContext: null,
                    cancellationToken: cancellationToken);

                continuationToken = results.ContinuationToken;
                foreach (var item in results.Results)
                {
                    if (!cancellationToken.IsCancellationRequested)
                    {
                        var blob = new CloudBlob(item.Uri, _blobClient);
                        await blob.FetchAttributesAsync();
                        logger.LogInformation(
                            "Found {BlobName} [{BlobType}], last modified at {LastModified}.",
                            blob.Name,
                            blob.BlobType,
                            blob.Properties.LastModified);

                        if (_scannerStrategy(blob))
                        {
                            logger.LogInformation(
                               "Creating new command for {BlobName} [{BlobType}].",
                               blob.Name,
                               blob.BlobType);

                            await commands.AddAsync(
                                new BlobArchiveCommand { BlobName = blob.Name },
                                cancellationToken);

                            blobsToBeArchived++;
                        }
                    }
                    blobsFound++;
                }
            }
            while (continuationToken != null);

            logger.LogInformation(
                "Found {BlobsFound} blobs, to be archived {BlobsToBeArchived}.",
                blobsFound,
                blobsToBeArchived);
        }
    }
}
