using System;
using System.Collections.Generic;
using System.IO;
using System.Reactive.Linq;
using System.Threading;
using System.Threading.Tasks;
using Duplicati.Library.Interface;
using Duplicati.Library.Utility;
using Minio;
using Minio.Exceptions;
using Minio.DataModel;
using System.Linq;

namespace Duplicati.Library.Backend
{
    public class S3MinioClient : IS3Client
    {
        private static readonly string Logtag = Logging.Log.LogTagFromType<S3MinioClient>();

        private MinioClient m_client;
        private readonly string m_locationConstraint;
        private readonly string m_dnsHost;

        public S3MinioClient(string awsID, string awsKey, string locationConstraint,
            string servername, string storageClass, bool useSSL, Dictionary<string, string> options)
        {
            m_locationConstraint = locationConstraint;
            m_client = new MinioClient(
                servername,
                awsID,
                awsKey,
                locationConstraint
            );

            if (useSSL)
            {
                m_client = m_client.WithSSL();
            }

            m_dnsHost = servername;
        }

        public async IAsyncEnumerable<IFileEntry> ListBucketAsync(string bucketName, string prefix, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancelToken)
        {
            ThrowExceptionIfBucketDoesNotExist(bucketName);

            var observable = m_client.ListObjectsAsync(bucketName, prefix, true, cancelToken);

            await foreach (var obj in observable.ToAsyncEnumerable())
            {
                yield return new Common.IO.FileEntry(
                    obj.Key,
                    (long)obj.Size,
                    Convert.ToDateTime(obj.LastModified),
                    Convert.ToDateTime(obj.LastModified)
                );
            }
        }

        public async Task AddBucketAsync(string bucketName, CancellationToken cancelToken)
        {
            try
            {
                await m_client.MakeBucketAsync(bucketName, m_locationConstraint, cancelToken);
            }
            catch (MinioException e)
            {
                Logging.Log.WriteErrorMessage(Logtag, "ErrorMakingBucketMinio", null,
                    "Error making bucket {0} using Minio: {1}", bucketName, e.ToString());
            }
        }

        public async Task DeleteObjectAsync(string bucketName, string keyName, CancellationToken cancelToken)
        {
            try
            {
                await m_client.RemoveObjectAsync(bucketName, keyName, cancelToken);
            }
            catch (MinioException e)
            {
                Logging.Log.WriteErrorMessage(Logtag, "ErrorRemovingObjectMinio", null,
                    "Error removing from bucket {0} object {1} using Minio: {1}",
                    bucketName, keyName, e.ToString());
            }
        }

        public async Task RenameFileAsync(string bucketName, string source, string target, CancellationToken cancelToken)
        {
            try
            {
                await m_client.CopyObjectAsync(bucketName, source,
                    bucketName, target, cancellationToken: cancelToken);
            }
            catch (MinioException e)
            {
                Logging.Log.WriteErrorMessage(Logtag, "ErrorCopyingObjectMinio", null,
                    "Error copying object {0} to {1} in bucket {2} using Minio: {3}",
                    source, target, bucketName, e.ToString());
                // Do not delete if copy failed (maybe should rethrow?)
                return;
            }
            await DeleteObjectAsync(bucketName, source, cancelToken);
        }

        public async Task GetFileStreamAsync(string bucketName, string keyName, Stream target, CancellationToken cancelToken)
        {
            try
            {
                // Check whether the object exists using statObject().
                // If the object is not found, statObject() throws an exception,
                // else it means that the object exists.
                // Execution is successful.
                await m_client.StatObjectAsync(bucketName, keyName, cancellationToken: cancelToken);

                // Get input stream to have content of 'my-objectname' from 'my-bucketname'
                await m_client.GetObjectAsync(bucketName, keyName,
                    (stream) => { Utility.Utility.CopyStream(stream, target); }, cancellationToken: cancelToken);
            }
            catch (MinioException e)
            {
                Logging.Log.WriteErrorMessage(Logtag, "ErrorGettingObjectMinio", null,
                    "Error getting object {0} to {1} using Minio: {2}",
                    keyName, bucketName, e.ToString());
            }
        }

        public string GetDnsHost()
        {
            return m_dnsHost;
        }

        public virtual async Task AddFileStreamAsync(string bucketName, string keyName, Stream source,
            CancellationToken cancelToken)
        {
            ThrowExceptionIfBucketDoesNotExist(bucketName);

            try
            {
                await m_client.PutObjectAsync(bucketName,
                    keyName,
                    source,
                    source.Length,
                    "application/octet-stream", cancellationToken: cancelToken);
            }
            catch (MinioException e)
            {
                Logging.Log.WriteErrorMessage(Logtag, "ErrorPuttingObjectMinio", null,
                    "Error putting object {0} to {1} using Minio: {2}",
                    keyName, bucketName, e.ToString());
            }
        }

        private void ThrowExceptionIfBucketDoesNotExist(string bucketName)
        {
            if (!m_client.BucketExistsAsync(bucketName).Await())
            {
                throw new FolderMissingException($"Bucket {bucketName} does not exist.");
            }
        }


        #region IDisposable Members

        public void Dispose()
        {
            m_client = null;
        }

        #endregion

    }
}