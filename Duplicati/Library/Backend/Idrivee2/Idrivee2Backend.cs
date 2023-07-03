#region Disclaimer / License
// Copyright (C) 2015, The Duplicati Team
// http://www.duplicati.com, info@duplicati.com
//
// This library is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public
// License as published by the Free Software Foundation; either
// version 2.1 of the License, or (at your option) any later version.
//
// This library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
// Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public
// License along with this library; if not, write to the Free Software
// Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
//
#endregion
using Duplicati.Library.Common.IO;
using Duplicati.Library.Interface;
using Duplicati.Library.Utility;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;

namespace Duplicati.Library.Backend
{
    public class Idrivee2Backend : IBackend, IBackendPagination, IRenameEnabledBackend
    {
        private static readonly string LOGTAG = Logging.Log.LogTagFromType<Idrivee2Backend>();

        static Idrivee2Backend()
        {

        }

        private readonly string m_prefix;
        private readonly string m_bucket;

        private IS3Client m_s3Client;

        public Idrivee2Backend()
        {
        }

        public Idrivee2Backend(string url, Dictionary<string, string> options)
        {
            var uri = new Utility.Uri(url);
            m_bucket = uri.Host;
            m_prefix = uri.Path;
            m_prefix = m_prefix.Trim();
            if (m_prefix.Length != 0)
            {
                m_prefix = Util.AppendDirSeparator(m_prefix, "/");
            }
            string accessKeyId = null;
            string accessKeySecret = null;

            if (options.ContainsKey("auth-username"))
                accessKeyId = options["auth-username"];
            if (options.ContainsKey("auth-password"))
                accessKeySecret = options["auth-password"];

            if (options.ContainsKey("access_key_id"))
                accessKeyId = options["access_key_id"];
            if (options.ContainsKey("secret_access_key"))
                accessKeySecret = options["secret_access_key"];

            if (string.IsNullOrEmpty(accessKeyId))
                throw new UserInformationException(Strings.Idrivee2Backend.NoKeyIdError, "Idrivee2NoKeyId");
            if (string.IsNullOrEmpty(accessKeySecret))
                throw new UserInformationException(Strings.Idrivee2Backend.NoKeySecretError, "Idrivee2NoKeySecret");
            string host = GetRegionEndpoint("https://api.idrivee2.com/api/service/get_region_end_point/" + accessKeyId);


            m_s3Client = new S3AwsClient(accessKeyId, accessKeySecret, null, host, null, true, options);

        }

        public string GetRegionEndpoint(string url)
        {
            try
            {
                // TODO: Reuse some existing HttpClient instead of creating new one
                using (var client = new HttpClient())
                using (HttpResponseMessage resp = client.GetAsync(url).Await())
                {
                    if (!resp.IsSuccessStatusCode)
                        throw new Exception("Failed to fetch region endpoint");

                    return resp.Content.ReadAsStringAsync().Await();
                }
            }
            catch (HttpRequestException)
            {
                //Convert to better exception
                throw new Exception("Failed to fetch region endpoint");
            }
        }

        #region IBackend Members

        public string DisplayName
        {
            get { return Strings.Idrivee2Backend.DisplayName; }
        }

        public string ProtocolKey => "e2";

        public bool SupportsStreaming => true;


        public Task<IList<IFileEntry>> ListAsync(CancellationToken cancelToken)
            => this.CondensePaginatedListAsync(cancelToken);

        public async Task PutAsync(string remotename, Stream input, CancellationToken cancelToken)
        {
            await Connection.AddFileStreamAsync(m_bucket, GetFullKey(remotename), input, cancelToken);
        }

        public async Task GetAsync(string remotename, Stream destination, CancellationToken cancelToken)
        {
            await Connection.GetFileStreamAsync(m_bucket, GetFullKey(remotename), destination, cancelToken);
        }

        public async Task DeleteAsync(string remotename, CancellationToken cancelToken)
        {
            await Connection.DeleteObjectAsync(m_bucket, GetFullKey(remotename), cancelToken);
        }

        public async Task TestAsync(CancellationToken cancelToken)
        {
            await this.TestListAsync(cancelToken);
        }

        public async Task CreateFolderAsync(CancellationToken cancelToken)
        {
            //S3 does not complain if the bucket already exists
            await Connection.AddBucketAsync(m_bucket, cancelToken);
        }

        public IList<ICommandLineArgument> SupportedCommands
        {
            get
            {

                var defaults = new Amazon.S3.AmazonS3Config();

                var exts =
                    typeof(Amazon.S3.AmazonS3Config).GetProperties().Where(x => x.CanRead && x.CanWrite && (x.PropertyType == typeof(string) || x.PropertyType == typeof(bool) || x.PropertyType == typeof(int) || x.PropertyType == typeof(long) || x.PropertyType.IsEnum))
                        .Select(x => (ICommandLineArgument)new CommandLineArgument(
                            "s3-ext-" + x.Name.ToLowerInvariant(),
                            x.PropertyType == typeof(bool) ? CommandLineArgument.ArgumentType.Boolean : x.PropertyType.IsEnum ? CommandLineArgument.ArgumentType.Enumeration : CommandLineArgument.ArgumentType.String,
                            x.Name,
                            string.Format("Extended option {0}", x.Name),
                            string.Format("{0}", x.GetValue(defaults)),
                            null,
                            x.PropertyType.IsEnum ? Enum.GetNames(x.PropertyType) : null));


                var normal = new ICommandLineArgument[] {

                    new CommandLineArgument("access_key_secret", CommandLineArgument.ArgumentType.Password, Strings.Idrivee2Backend.KeySecretDescriptionShort, Strings.Idrivee2Backend.KeySecretDescriptionLong, null, new[]{"auth-password"}, null),
                    new CommandLineArgument("access_key_id", CommandLineArgument.ArgumentType.String, Strings.Idrivee2Backend.KeyIDDescriptionShort, Strings.Idrivee2Backend.KeyIDDescriptionLong,null, new[]{"auth-username"}, null)

                };

                return normal.Union(exts).ToList();

            }
        }

        public string Description
        {
            get
            {
                return Strings.Idrivee2Backend.Description;
            }
        }

        #endregion

        public async IAsyncEnumerable<IFileEntry> ListEnumerableAsync([System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancelToken)
        {
            await foreach (IFileEntry file in Connection.ListBucketAsync(m_bucket, m_prefix, cancelToken))
            {
                ((FileEntry)file).Name = file.Name.Substring(m_prefix.Length);
                if (file.Name.StartsWith("/", StringComparison.Ordinal) && !m_prefix.StartsWith("/", StringComparison.Ordinal))
                    ((FileEntry)file).Name = file.Name.Substring(1);

                yield return file;
            }
        }


        #region IRenameEnabledBackend Members

        public async Task RenameAsync(string source, string target, CancellationToken cancelToken)
        {
            await Connection.RenameFileAsync(m_bucket, GetFullKey(source), GetFullKey(target), cancelToken);
        }

        #endregion

        #region IDisposable Members

        public void Dispose()
        {
            m_s3Client?.Dispose();
            m_s3Client = null;
        }

        #endregion

        private IS3Client Connection => m_s3Client;

        public string[] DNSName
        {
            get { return new[] { m_s3Client.GetDnsHost() }; }
        }

        private string GetFullKey(string name)
        {
            //AWS SDK encodes the filenames correctly
            return m_prefix + name;
        }
    }
}
