//  Copyright (C) 2015, The Duplicati Team
//  http://www.duplicati.com, info@duplicati.com
//
//  This library is free software; you can redistribute it and/or modify
//  it under the terms of the GNU Lesser General Public License as
//  published by the Free Software Foundation; either version 2.1 of the
//  License, or (at your option) any later version.
//
//  This library is distributed in the hope that it will be useful, but
//  WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
//  Lesser General Public License for more details.
//
//  You should have received a copy of the GNU Lesser General Public
//  License along with this library; if not, write to the Free Software
//  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
using Duplicati.Library.Backend.GoogleServices;
using Duplicati.Library.Common.IO;
using Duplicati.Library.Interface;
using Duplicati.Library.Utility;
using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Duplicati.Library.Backend.GoogleCloudStorage
{
    // ReSharper disable once UnusedMember.Global
    // This class is instantiated dynamically in the BackendLoader.
    public class GoogleCloudStorage : IBackend, IBackendPagination, IRenameEnabledBackend
    {
        private const string AUTHID_OPTION = "authid";
        private const string PROJECT_OPTION = "gcs-project";

        private const string LOCATION_OPTION = "gcs-location";
        private const string STORAGECLASS_OPTION = "gcs-storage-class";

        private readonly string m_bucket;
        private readonly string m_prefix;
        private readonly string m_project;
        private readonly OAuthHelper m_oauth;

        private readonly string m_location;
        private readonly string m_storage_class;

        public GoogleCloudStorage()
        {
        }

        public GoogleCloudStorage(string url, Dictionary<string, string> options)
        {
            var uri = new Utility.Uri(url);

            m_bucket = uri.Host;
            m_prefix = Util.AppendDirSeparator("/" + uri.Path, "/");

            // For GCS we do not use a leading slash
            if (m_prefix.StartsWith("/", StringComparison.Ordinal))
                m_prefix = m_prefix.Substring(1);

            string authid;
            options.TryGetValue(AUTHID_OPTION, out authid);
            options.TryGetValue(PROJECT_OPTION, out m_project);
            options.TryGetValue(LOCATION_OPTION, out m_location);
            options.TryGetValue(STORAGECLASS_OPTION, out m_storage_class);

            if (string.IsNullOrEmpty(authid))
                throw new UserInformationException(Strings.GoogleCloudStorage.MissingAuthID(AUTHID_OPTION), "GoogleCloudStorageMissingAuthID");

            m_oauth = new OAuthHelper(authid, this.ProtocolKey);
            m_oauth.AutoAuthHeader = true;
        }


        private class ListBucketResponse
        {
            public string nextPageToken { get; set; }
            public BucketResourceItem[] items { get; set; }
        }

        private class BucketResourceItem
        {
            public string name { get; set; }
            public DateTime? updated { get; set; }
            public long? size { get; set; }
        }

        private class CreateBucketRequest
        {
            public string name { get; set; }
            public string location { get; set; }
            public string storageClass { get; set; }
        }

        private async Task<T> HandleListExceptions<T>(Func<Task<T>> func)
        {
            try
            {
                return await func();
            }
            catch (WebException wex)
            {
                if (wex.Response is HttpWebResponse response && response.StatusCode == HttpStatusCode.NotFound)
                    throw new FolderMissingException();
                else
                    throw;
            }
        }

        #region IBackend implementation
        public Task<IList<IFileEntry>> ListAsync(CancellationToken cancelToken)
            => this.CondensePaginatedListAsync(cancelToken);

        public async Task GetAsync(string remotename, Stream destination, CancellationToken cancelToken)
        {
            var url = WebApi.GoogleCloudStorage.GetUrl(m_bucket, Library.Utility.Uri.UrlPathEncode(m_prefix + remotename));
            var req = await m_oauth.CreateRequestAsync(url, "GET", cancelToken);

            using (var resp = await m_oauth.GetResponseAsync(req, null, cancelToken))
                if (resp.StatusCode == HttpStatusCode.NotFound)
                {
                    throw new FileMissingException();
                }
                else if (!resp.IsSuccessStatusCode)
                {
                    throw new HttpRequestStatusException(resp);
                }
                else
                {
                    using (var rs = await resp.Content.ReadAsStreamAsync())
                        Library.Utility.Utility.CopyStream(rs, destination);
                }
        }

        public async Task DeleteAsync(string remotename, CancellationToken cancelToken)
        {
            var req = await m_oauth.CreateRequestAsync(
                WebApi.GoogleCloudStorage.DeleteUrl(m_bucket, Library.Utility.Uri.UrlPathEncode(m_prefix + remotename)),
                "DELETE", cancelToken);

            await m_oauth.ReadJSONResponseAsync<object>(req, cancelToken);
        }

        public Task TestAsync(CancellationToken cancelToken)
            => this.TestListAsync(cancelToken);

        public async Task CreateFolderAsync(CancellationToken cancelToken)
        {
            if (string.IsNullOrEmpty(m_project))
                throw new UserInformationException(Strings.GoogleCloudStorage.ProjectIDMissingError(PROJECT_OPTION), "GoogleCloudStorageMissingProjectID");

            var data = new CreateBucketRequest
            {
                name = m_bucket,
                location = m_location,
                storageClass = m_storage_class
            };

            await m_oauth.ReadJSONResponseAsync<BucketResourceItem>(WebApi.GoogleCloudStorage.CreateFolderUrl(m_project), data, "POST", cancelToken);
        }

        public async IAsyncEnumerable<IFileEntry> ListEnumerableAsync([EnumeratorCancellation] CancellationToken cancelToken)
        {
            var url = WebApi.GoogleCloudStorage.ListUrl(m_bucket, Utility.Uri.UrlEncode(m_prefix));
            while (true)
            {
                var resp = await HandleListExceptions(async () => await m_oauth.ReadJSONResponseAsync<ListBucketResponse>(url, null, "GET", cancelToken));

                if (resp.items != null)
                    foreach (var f in resp.items)
                    {
                        var name = f.name;
                        if (name.StartsWith(m_prefix, StringComparison.OrdinalIgnoreCase))
                            name = name.Substring(m_prefix.Length);
                        if (f.size == null)
                            yield return new FileEntry(name);
                        else if (f.updated == null)
                            yield return new FileEntry(name, f.size.Value);
                        else
                            yield return new FileEntry(name, f.size.Value, f.updated.Value, f.updated.Value);
                    }

                var token = resp.nextPageToken;
                if (string.IsNullOrWhiteSpace(token))
                    break;
                url = WebApi.GoogleCloudStorage.ListUrl(m_bucket, Utility.Uri.UrlEncode(m_prefix), token);

            }
        }

        public async Task PutAsync(string remotename, System.IO.Stream stream, CancellationToken cancelToken)
        {
            var item = new BucketResourceItem { name = m_prefix + remotename };

            var url = WebApi.GoogleCloudStorage.PutUrl(m_bucket);
            var res = await GoogleCommon.ChunkedUploadWithResumeAsync<BucketResourceItem, BucketResourceItem>(m_oauth, item, url, stream, cancelToken);

            if (res == null)
                throw new Exception("Upload succeeded, but no data was returned");
        }

        public string DisplayName
        {
            get { return Strings.GoogleCloudStorage.DisplayName; }
        }

        public string ProtocolKey
        {
            get { return "gcs"; }
        }

        public IList<ICommandLineArgument> SupportedCommands
        {
            get
            {
                StringBuilder locations = new StringBuilder();
                StringBuilder storageClasses = new StringBuilder();

                foreach (KeyValuePair<string, string> s in WebApi.GoogleCloudStorage.KNOWN_GCS_LOCATIONS)
                    locations.AppendLine(string.Format("{0}: {1}", s.Key, s.Value));
                foreach (KeyValuePair<string, string> s in WebApi.GoogleCloudStorage.KNOWN_GCS_STORAGE_CLASSES)
                    storageClasses.AppendLine(string.Format("{0}: {1}", s.Key, s.Value));

                return new List<ICommandLineArgument>(new ICommandLineArgument[] {
                    new CommandLineArgument(LOCATION_OPTION, CommandLineArgument.ArgumentType.String, Strings.GoogleCloudStorage.LocationDescriptionShort, Strings.GoogleCloudStorage.LocationDescriptionLong(locations.ToString())),
                    new CommandLineArgument(STORAGECLASS_OPTION, CommandLineArgument.ArgumentType.String, Strings.GoogleCloudStorage.StorageclassDescriptionShort, Strings.GoogleCloudStorage.StorageclassDescriptionLong(storageClasses.ToString())),
                    new CommandLineArgument(AUTHID_OPTION, CommandLineArgument.ArgumentType.Password, Strings.GoogleCloudStorage.AuthidShort, Strings.GoogleCloudStorage.AuthidLong(OAuthHelper.OAUTH_LOGIN_URL("gcs"))),
                    new CommandLineArgument(PROJECT_OPTION, CommandLineArgument.ArgumentType.String, Strings.GoogleCloudStorage.ProjectDescriptionShort, Strings.GoogleCloudStorage.ProjectDescriptionLong),
                });
            }
        }
        public string Description
        {
            get { return Strings.GoogleCloudStorage.Description; }
        }

        public string[] DNSName
        {
            get
            {
                return WebApi.GoogleCloudStorage.Hosts();
            }
        }

        public bool SupportsStreaming => true;

        #endregion

        public async Task RenameAsync(string oldname, string newname, CancellationToken cancelToken)
        {
            var data = new BucketResourceItem
            {
                name = m_prefix + newname,
            };

            var url = WebApi.GoogleCloudStorage.RenameUrl(m_bucket, Utility.Uri.UrlPathEncode(m_prefix + oldname));
            await m_oauth.ReadJSONResponseAsync<BucketResourceItem>(url, data, "PATCH", CancellationToken.None);
        }

        #region IDisposable implementation
        public void Dispose()
        {

        }
        #endregion
    }
}

