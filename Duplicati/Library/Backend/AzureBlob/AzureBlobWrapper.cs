// Copyright (C) 2024, The Duplicati Team
// https://duplicati.com, hello@duplicati.com
// 
// Permission is hereby granted, free of charge, to any person obtaining a 
// copy of this software and associated documentation files (the "Software"), 
// to deal in the Software without restriction, including without limitation 
// the rights to use, copy, modify, merge, publish, distribute, sublicense, 
// and/or sell copies of the Software, and to permit persons to whom the 
// Software is furnished to do so, subject to the following conditions:
// 
// The above copyright notice and this permission notice shall be included in 
// all copies or substantial portions of the Software.
// 
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS 
// OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, 
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE 
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER 
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING 
// FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER 
// DEALINGS IN THE SOFTWARE.

using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Duplicati.Library.Common.IO;
using Duplicati.Library.Interface;
using Duplicati.Library.Utility;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;

namespace Duplicati.Library.Backend.AzureBlob
{
    /// <summary>
    /// Azure blob storage facade.
    /// </summary>
    public class AzureBlobWrapper
    {
        private readonly string _containerName;
        private readonly CloudBlobContainer _container;
        private readonly OperationContext _operationContext;
        
        // Note: May need metadata; need to test with Azure blobs
        private const BlobListingDetails ListDetails = BlobListingDetails.None;

        public string[] DnsNames
        {
            get
            {
                var lst = new List<string>();
                if (_container != null)
                {
                    if (_container.Uri != null)
                        lst.Add(_container.Uri.Host);

                    if (_container.StorageUri != null)
                    {
                        if (_container.StorageUri.PrimaryUri != null)
                            lst.Add(_container.StorageUri.PrimaryUri.Host);
                        if (_container.StorageUri.SecondaryUri != null)
                            lst.Add(_container.StorageUri.SecondaryUri.Host);
                    }
                }

                return lst.ToArray();
            }
        }

        public AzureBlobWrapper(string accountName, string accessKey, string sasToken, string containerName)
        {
            _operationContext = new() { 
                CustomUserAgent = string.Format(
                    "APN/1.0 Duplicati/{0} AzureBlob/2.0 {1}",
                    System.Reflection.Assembly.GetExecutingAssembly().GetName().Version,
                    Microsoft.WindowsAzure.Storage.Shared.Protocol.Constants.HeaderConstants.UserAgent
            )};

            string connectionString;
            if (sasToken != null)
            {
                connectionString = string.Format("DefaultEndpointsProtocol=https;AccountName={0};SharedAccessSignature={1}",
                    accountName, sasToken);
            }
            else
            {
                connectionString = string.Format("DefaultEndpointsProtocol=https;AccountName={0};AccountKey={1}",
                            accountName, accessKey);
            }

            var storageAccount = CloudStorageAccount.Parse(connectionString);
            var blobClient = storageAccount.CreateCloudBlobClient();

            _containerName = containerName;
            _container = blobClient.GetContainerReference(containerName);
        }

        public async Task AddContainerAsync(CancellationToken cancelToken)
        {
            await _container.CreateAsync(default(BlobContainerPublicAccessType), default(BlobRequestOptions), _operationContext, cancelToken);
            await _container.SetPermissionsAsync(new BlobContainerPermissions { PublicAccess = BlobContainerPublicAccessType.Off }, default(AccessCondition), default(BlobRequestOptions), _operationContext, cancelToken);
        }

        public virtual Task GetFileStreamAsync(string keyName, Stream target, CancellationToken cancelToken)
            => _container.GetBlockBlobReference(keyName).DownloadToStreamAsync(target, default(AccessCondition), default(BlobRequestOptions), _operationContext, cancelToken);


        public virtual Task AddFileStream(string keyName, Stream source, CancellationToken cancelToken)
            => _container.GetBlockBlobReference(keyName).UploadFromStreamAsync(source, source.Length, default(AccessCondition), default(BlobRequestOptions), _operationContext, cancelToken);

        public Task DeleteObjectAsync(string keyName, CancellationToken cancelToken)
            =>  _container.GetBlockBlobReference(keyName).DeleteIfExistsAsync(default(DeleteSnapshotsOption), default(AccessCondition), default(BlobRequestOptions), _operationContext, cancelToken);

        private async Task<List<IListBlobItem>> ListBlobEntriesAsync(CancellationToken cancelToken)
        {
            var segment = await _container.ListBlobsSegmentedAsync(null, false, ListDetails, null, null, default(BlobRequestOptions), _operationContext, cancelToken);
            var list = new List<IListBlobItem>();

            list.AddRange(segment.Results);

            while (segment.ContinuationToken != null)
            {
                // TODO-DNC do we need BlobListingDetails.Metadata ???
                segment = await _container.ListBlobsSegmentedAsync(null, false, ListDetails, null,  segment.ContinuationToken, default(BlobRequestOptions), _operationContext, cancelToken);
                list.AddRange(segment.Results);
            }

            return list;
        }

        public virtual async Task<List<IFileEntry>> ListContainerEntriesAsync(CancellationToken cancelToken)
        {
            var listBlobItems = await ListBlobEntriesAsync(cancelToken);
            try
            {
                return listBlobItems.Select(x =>
                {
                    var absolutePath = x.StorageUri.PrimaryUri.AbsolutePath;
                    var containerSegment = string.Concat("/", _containerName, "/");
                    var blobName = absolutePath.Substring(absolutePath.IndexOf(
                        containerSegment, System.StringComparison.Ordinal) + containerSegment.Length);

                    try
                    {
                        if (x is CloudBlockBlob cb)
                        {
                            var lastModified = new System.DateTime();
                            if (cb.Properties.LastModified != null)
                                lastModified = new System.DateTime(cb.Properties.LastModified.Value.Ticks, System.DateTimeKind.Utc);
                            return new FileEntry(Uri.UrlDecode(blobName.Replace("+", "%2B")), cb.Properties.Length, lastModified, lastModified);
                        }
                    }
                    catch
                    {
                        // If the metadata fails to parse, return the basic entry
                    }

                    return new FileEntry(Uri.UrlDecode(blobName.Replace("+", "%2B")));
                })
                .Cast<IFileEntry>()
                .ToList();
            }
            catch (StorageException ex)
            {
                if (ex.RequestInformation.HttpStatusCode == 404)
                {
                    throw new FolderMissingException(ex);
                }
                throw;
            }
        }
    }
}
