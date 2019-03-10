using Microsoft.WindowsAzure.Storage;
using System;
using System.Text;
using System.Threading.Tasks;

namespace AppendBlobUtility
{
    class Program
    {
        private static Random _random = new Random();

        private const string SampleText = "Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, sed diam voluptua.";
        private const string ContainerName = "source";

        // Insert a Azure Storage connection string here.
        // Note that the Storage Emulator as of v5.7 does not support Append Blobs! 
        private const string ConnetionString = "<StorageAccountConnectionString>";

        static async Task Main(string[] args)
        {
            var storageAccount = CloudStorageAccount.Parse(ConnetionString);
            var blobClient = storageAccount.CreateCloudBlobClient();
            var container = blobClient.GetContainerReference(ContainerName);

            for (int i = 1; i <= 10; i++)
            {
                string text = CreateRandomText();
                var appendBlob = container.GetAppendBlobReference($"appendblob_{i}.txt");
                await appendBlob.CreateOrReplaceAsync();
                await appendBlob.AppendTextAsync(text);
                appendBlob.Properties.ContentType = "text/plain";
                await appendBlob.SetPropertiesAsync();
            }

            Console.WriteLine("Done. Press any key to exit.");
            Console.ReadKey(true);
        }

        private static string CreateRandomText()
        {
            var stringBuilder = new StringBuilder();
            int lines = _random.Next(20, 100);
            for (int i = 0; i < lines; i++)
            {
                stringBuilder.AppendLine(SampleText);
            }
            return stringBuilder.ToString();
        }
    }
}
