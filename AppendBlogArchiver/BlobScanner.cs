using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace AppendBlogArchiver
{
    public class BlobScanner
    {
        private readonly Func<CloudBlob, bool> _scannerStrategy;
        private readonly CloudBlobClient _blobClient;
        private const string ContainerName = "source";

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
        //public BlobScanner(CloudBlobClient blobClient, Func<CloudBlob, bool> scannerStrategy, IConfiguration configuration)
        //{
        //    _sourceContainer = sourceContainer ?? throw new ArgumentNullException(nameof(sourceContainer)); 
        //    _scannerStrategy = scannerStrategy ?? throw new ArgumentNullException(nameof(scannerStrategy));
        //    _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
        //}

        [FunctionName(nameof(BlobScanner))]
        public async Task Run(
            [TimerTrigger("0 */2 * * * *")]TimerInfo timer,
            [Queue("archivecommands", Connection = "StorageConnectionString")]IAsyncCollector<BlobArchiveCommand> commands,
            ILogger logger,
            CancellationToken cancellationToken)
        {
            logger.LogInformation($"{nameof(BlobScanner)} function executed at: {DateTime.Now}.");

            var container = _blobClient.GetContainerReference(ContainerName);
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
                "Found {BlobsScanned} blobs, to be archived {BlobsProcessed}.",
                blobsFound,
                blobsToBeArchived);
        }
    }
}
