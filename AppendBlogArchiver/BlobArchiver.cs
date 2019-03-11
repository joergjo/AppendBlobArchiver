using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.IO;
using System.Threading.Tasks;

namespace AppendBlogArchiver
{
    public class BlobArchiver
    {
        private readonly Func<ICloudBlob, ICloudBlob, bool> _ignoreDuplicateStrategy;

        public BlobArchiver()
        {
            _ignoreDuplicateStrategy = (srcBlob, archiveBlob) =>
            {
                return (srcBlob.Name == archiveBlob.Name);
            };
        }

        [FunctionName(nameof(BlobArchiver))]
        public async Task Run(
            [QueueTrigger("archivecommands", Connection = "StorageConnectionString")]BlobArchiveCommand command,
            [Blob("source/{BlobName}", FileAccess.ReadWrite, Connection = "StorageConnectionString")]CloudAppendBlob sourceBlob,
            [Blob("archive/{BlobName}", FileAccess.ReadWrite, Connection = "StorageConnectionString")]CloudBlockBlob archiveBlob,
            ILogger logger)
        {
            logger.LogInformation("BlobArchiver received command to archive blob {BlobName}.", command.BlobName);

            if (!await archiveBlob.ExistsAsync() ||
                !_ignoreDuplicateStrategy(sourceBlob, archiveBlob))
            {
                using (var stream = await sourceBlob.OpenReadAsync(
                    accessCondition: null,
                    options: null,
                    operationContext: null))
                {
                    await archiveBlob.UploadFromStreamAsync(stream);
                    logger.LogInformation("Copied blob {BlobName} to archive.", command.BlobName);
                }
            }
            else
            {
                logger.LogWarning("Blob {BlobName} already exists in archive container.", command.BlobName);
            }

            await sourceBlob.DeleteAsync();
            logger.LogInformation("Deleted blob {BlobName}.", command.BlobName);
        }
    }
}
