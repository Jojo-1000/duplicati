using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Duplicati.Library.Interface;

namespace Duplicati.Library.Backend
{
    public interface IS3Client : IDisposable
    {
        IAsyncEnumerable<IFileEntry> ListBucketAsync(string bucketName, string prefix, CancellationToken cancelToken);

        Task AddBucketAsync(string bucketName, CancellationToken cancelToken);

        Task DeleteObjectAsync(string bucketName, string keyName, CancellationToken cancelToken);

        Task RenameFileAsync(string bucketName, string source, string target, CancellationToken cancelToken);

        Task GetFileStreamAsync(string bucketName, string keyName, System.IO.Stream target, CancellationToken cancelToken);

        string GetDnsHost();

        Task AddFileStreamAsync(string bucketName, string keyName, System.IO.Stream source, CancellationToken cancelToken);
    }
}