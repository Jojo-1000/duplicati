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
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Duplicati.Library.Backend.GoogleDrive
{
    // ReSharper disable once UnusedMember.Global
    // This class is instantiated dynamically in the BackendLoader.
    public class GoogleDrive : IBackend, IBackendPagination, IQuotaEnabledBackend, IRenameEnabledBackend
    {
        private const string AUTHID_OPTION = "authid";
        private const string TEAMDRIVE_ID = "googledrive-teamdrive-id";
        private const string FOLDER_MIMETYPE = "application/vnd.google-apps.folder";

        private readonly string m_path;
        private readonly string m_teamDriveID;
        private readonly OAuthHelper m_oauth;
        private readonly Dictionary<string, GoogleDriveFolderItem[]> m_filecache;

        private string m_currentFolderId;

        public GoogleDrive()
        {
        }

        public GoogleDrive(string url, Dictionary<string, string> options)
        {
            var uri = new Utility.Uri(url);

            m_path = Util.AppendDirSeparator(uri.HostAndPath, "/");

            string authid = null;
            if (options.ContainsKey(AUTHID_OPTION))
                authid = options[AUTHID_OPTION];

            if (options.ContainsKey(TEAMDRIVE_ID))
                m_teamDriveID = options[TEAMDRIVE_ID];

            m_oauth = new OAuthHelper(authid, this.ProtocolKey) { AutoAuthHeader = true };
            m_filecache = new Dictionary<string, GoogleDriveFolderItem[]>();
        }

        private async Task<string> GetFolderIdAsync(string path, CancellationToken cancelToken, bool autocreate = false)
        {
            var curparent = m_teamDriveID ?? GetAboutInfo().rootFolderId;
            var curdisplay = new StringBuilder("/");

            foreach (var p in path.Split(new char[] { '/' }, StringSplitOptions.RemoveEmptyEntries))
            {
                var res = new List<GoogleDriveFolderItem>();
                await foreach (var i in ListFolderAsync(curparent, cancelToken, true, p))
                {
                    res.Add(i);
                    if (res.Count > 1)
                    {
                        // Do not need to list all folders, more than one is an error
                        break;
                    }
                }

                if (res.Count == 0)
                {
                    if (!autocreate)
                        throw new FolderMissingException();

                    curparent = (await CreateFolderAsync(p, curparent, cancelToken)).id;
                }
                else if (res.Count > 1)
                {
                    throw new UserInformationException(Strings.GoogleDrive.MultipleEntries(p, curdisplay.ToString()), "GoogleDriveMultipleEntries");
                }
                else
                {
                    curparent = res[0].id;
                }

                curdisplay.Append(p).Append("/");
            }

            return curparent;
        }

        private string CurrentFolderId
        {
            get
            {
                if (string.IsNullOrEmpty(m_currentFolderId))
                    m_currentFolderId = GetFolderIdAsync(m_path, CancellationToken.None).Result;

                return m_currentFolderId;
            }
        }

        private async Task<GoogleDriveFolderItem[]> GetFileEntriesAsync(string remotename, CancellationToken cancelToken, bool throwMissingException = true)
        {
            GoogleDriveFolderItem[] entries;
            m_filecache.TryGetValue(remotename, out entries);

            if (entries != null)
                return entries;


            var lst = new List<GoogleDriveFolderItem>();
            await foreach (var i in ListFolderAsync(CurrentFolderId, cancelToken, false, remotename))
                lst.Add(i);

            entries = lst.ToArray();

            if (entries == null || entries.Length == 0)
            {
                if (throwMissingException)
                    throw new FileMissingException();
                else
                    return null;
            }

            return m_filecache[remotename] = entries;
        }

        private static string EscapeTitleEntries(string title)
        {
            return title.Replace("'", "\\'");
        }

        #region IBackend implementation

        public async Task PutAsync(string remotename, System.IO.Stream stream, CancellationToken cancelToken)
        {
            try
            {
                // Figure out if we update or create the file
                if (m_filecache.Count == 0)
                    await foreach (var file in ListEnumerableAsync(cancelToken)) { /* Enumerate the full listing */ }

                GoogleDriveFolderItem[] files;
                m_filecache.TryGetValue(remotename, out files);

                string fileId = null;
                if (files != null)
                {
                    if (files.Length == 1)
                        fileId = files[0].id;
                    else
                        await DeleteAsync(remotename, cancelToken);
                }

                var isUpdate = !string.IsNullOrWhiteSpace(fileId);

                var url = WebApi.GoogleDrive.PutUrl(fileId, m_teamDriveID != null);

                var item = new GoogleDriveFolderItem
                {
                    title = remotename,
                    description = remotename,
                    mimeType = "application/octet-stream",
                    labels = new GoogleDriveFolderItemLabels { hidden = true },
                    parents = new GoogleDriveParentReference[] { new GoogleDriveParentReference { id = CurrentFolderId } },
                    teamDriveId = m_teamDriveID
                };

                var res = await GoogleCommon.ChunkedUploadWithResumeAsync<GoogleDriveFolderItem, GoogleDriveFolderItem>(m_oauth, item, url, stream, cancelToken, isUpdate ? "PUT" : "POST");
                m_filecache[remotename] = new GoogleDriveFolderItem[] { res };
            }
            catch
            {
                m_filecache.Clear();
                throw;
            }
        }

        public Task<IList<IFileEntry>> ListAsync(CancellationToken cancelToken)
            => this.CondensePaginatedListAsync(cancelToken);

        public async Task GetAsync(string remotename, Stream destination, CancellationToken cancelToken)
        {
            // Prevent repeated download url lookups
            if (m_filecache.Count == 0)
                await foreach (var file in ListEnumerableAsync(cancelToken)) { /* Enumerate the full listing */ }

            var fileId = (await GetFileEntriesAsync(remotename, cancelToken)).OrderByDescending(x => x.createdDate).First().id;

            var req = await m_oauth.CreateRequestAsync(WebApi.GoogleDrive.GetUrl(fileId), "GET", cancelToken);
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
            try
            {
                foreach (var fileid in from n in await GetFileEntriesAsync(remotename, cancelToken)
                                       select n.id)
                {
                    var url = WebApi.GoogleDrive.DeleteUrl(Library.Utility.Uri.UrlPathEncode(fileid), m_teamDriveID);
                    await m_oauth.GetJSONDataAsync<object>(url, x =>
                    {
                        x.Method = System.Net.Http.HttpMethod.Delete;
                    }, cancelToken);
                }

                m_filecache.Remove(remotename);
            }
            catch
            {
                m_filecache.Clear();

                throw;
            }
        }

        public Task TestAsync(CancellationToken cancelToken)
            => this.TestListAsync(cancelToken);

        public async Task CreateFolderAsync(CancellationToken cancelToken)
        {
            m_filecache.Clear();
            m_currentFolderId = await GetFolderIdAsync(m_path, cancelToken, true);
        }

        public async Task PutAsync(string remotename, string filename, CancellationToken cancelToken)
        {
            using (System.IO.FileStream fs = System.IO.File.OpenRead(filename))
                await PutAsync(remotename, fs, cancelToken);
        }

        public string DisplayName
        {
            get
            {
                return Strings.GoogleDrive.DisplayName;
            }
        }

        public string ProtocolKey
        {
            get
            {
                return "googledrive";
            }
        }

        public System.Collections.Generic.IList<ICommandLineArgument> SupportedCommands => new List<ICommandLineArgument>(new ICommandLineArgument[] {
                    new CommandLineArgument(AUTHID_OPTION,
                                            CommandLineArgument.ArgumentType.Password,
                                            Strings.GoogleDrive.AuthidShort,
                                            Strings.GoogleDrive.AuthidLong(OAuthHelper.OAUTH_LOGIN_URL("googledrive"))),
                    new CommandLineArgument(TEAMDRIVE_ID,
                                            CommandLineArgument.ArgumentType.String,
                                            Strings.GoogleDrive.TeamDriveIdShort,
                                            Strings.GoogleDrive.TeamDriveIdLong),
                });

        public string Description
        {
            get
            {
                return Strings.GoogleDrive.Description;
            }
        }

        #endregion

        #region IQuotaEnabledBackend implementation
        public IQuotaInfo Quota
        {
            get
            {
                try
                {
                    GoogleDriveAboutResponse about = this.GetAboutInfo();
                    return new QuotaInfo(about.quotaBytesTotal ?? -1, about.quotaBytesTotal - about.quotaBytesUsed ?? -1);
                }
                catch
                {
                    return null;
                }
            }
        }

        public string[] DNSName
        {
            get { return WebApi.GoogleDrive.Hosts(); }
        }

        public bool SupportsStreaming => true;

        #endregion

        #region IRenameEnabledBackend implementation

        public async Task RenameAsync(string oldname, string newname, CancellationToken cancelToken)
        {
            try
            {
                var files = await GetFileEntriesAsync(oldname, cancelToken, true);
                if (files.Length > 1)
                    throw new UserInformationException(string.Format(Strings.GoogleDrive.MultipleEntries(oldname, m_path)),
                                                       "GoogleDriveMultipleEntries");

                // TODO: Use PATCH request with fileId to rename
                Stream stream = new MemoryStream();
                await GetAsync(oldname, stream, cancelToken);
                await PutAsync(newname, stream, cancelToken);
                await DeleteAsync(oldname, cancelToken);

                m_filecache.Remove(oldname);
            }
            catch
            {
                m_filecache.Clear();

                throw;
            }

        }
        #endregion

        public async IAsyncEnumerable<IFileEntry> ListEnumerableAsync([System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancelToken)
        {
            bool success = false;
            try
            {
                m_filecache.Clear();

                // For now, this class assumes that List() fully populates the file cache
                await foreach (var n in ListFolderAsync(CurrentFolderId, cancelToken))
                {
                    FileEntry fe = null;

                    if (n.fileSize == null)
                        fe = new FileEntry(n.title);
                    else if (n.modifiedDate == null)
                        fe = new FileEntry(n.title, n.fileSize.Value);
                    else
                        fe = new FileEntry(n.title, n.fileSize.Value, n.modifiedDate.Value, n.modifiedDate.Value);

                    if (fe != null)
                    {
                        fe.IsFolder = FOLDER_MIMETYPE.Equals(n.mimeType, StringComparison.OrdinalIgnoreCase);

                        if (!fe.IsFolder)
                        {
                            GoogleDriveFolderItem[] lst;
                            if (!m_filecache.TryGetValue(fe.Name, out lst))
                            {
                                m_filecache[fe.Name] = new GoogleDriveFolderItem[] { n };
                            }
                            else
                            {
                                Array.Resize(ref lst, lst.Length + 1);
                                lst[lst.Length - 1] = n;
                            }
                        }

                        yield return fe;
                    }
                }

                success = true;
            }
            finally
            {
                // If the enumeration either failed or didn't complete, clear the file cache.
                // This way, other operations which require a fully populated file cache will see an empty one and can populate it themselves.
                if (!success)
                {
                    m_filecache.Clear();
                }
            }
        }

        #region IDisposable implementation

        public void Dispose()
        {
        }

        #endregion

        private class GoogleDriveParentReference
        {
            public string id { get; set; }
        }

        private class GoogleDriveListResponse
        {
            public string nextPageToken { get; set; }
            public GoogleDriveFolderItem[] items { get; set; }
        }

        private class GoogleDriveFolderItemLabels
        {
            public bool hidden { get; set; }
        }

        private class GoogleDriveFolderItem
        {
            public string id { get; set; }
            public string title { get; set; }
            public string description { get; set; }
            public string mimeType { get; set; }
            public GoogleDriveFolderItemLabels labels { get; set; }
            public DateTime? createdDate { get; set; }
            public DateTime? modifiedDate { get; set; }
            public long? fileSize { get; set; }
            public string teamDriveId { get; set; }
            public GoogleDriveParentReference[] parents { get; set; }
        }

        private class GoogleDriveAboutResponse
        {
            public long? quotaBytesTotal { get; set; }
            public long? quotaBytesUsed { get; set; }
            public string rootFolderId { get; set; }
        }

        private async IAsyncEnumerable<GoogleDriveFolderItem> ListFolderAsync(string parentfolder, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancelToken, bool? onlyFolders = null, string name = null)
        {
            var fileQuery = new string[] {
                string.IsNullOrEmpty(name) ? null : string.Format("title = '{0}'", EscapeTitleEntries(name)),
                onlyFolders == null ? null : string.Format("mimeType {0}= '{1}'", onlyFolders.Value ? "" : "!", FOLDER_MIMETYPE),
                string.Format("'{0}' in parents", EscapeTitleEntries(parentfolder)),
                "trashed=false"
            };

            var encodedFileQuery = Library.Utility.Uri.UrlEncode(string.Join(" and ", fileQuery.Where(x => x != null)));
            var url = WebApi.GoogleDrive.ListUrl(encodedFileQuery, m_teamDriveID);

            while (true)
            {
                var res = await m_oauth.GetJSONDataAsync<GoogleDriveListResponse>(url, cancelToken);
                foreach (var n in res.items)
                    yield return n;

                var token = res.nextPageToken;
                if (string.IsNullOrWhiteSpace(token))
                    break;

                url = WebApi.GoogleDrive.ListUrl(encodedFileQuery, m_teamDriveID, token);
            }
        }

        private GoogleDriveAboutResponse GetAboutInfo()
        {
            return m_oauth.GetJSONDataAsync<GoogleDriveAboutResponse>(WebApi.GoogleDrive.AboutInfoUrl(), CancellationToken.None).Result;
        }

        private async Task<GoogleDriveFolderItem> CreateFolderAsync(string name, string parent, CancellationToken cancelToken)
        {
            var folder = new GoogleDriveFolderItem()
            {
                title = name,
                description = name,
                mimeType = FOLDER_MIMETYPE,
                labels = new GoogleDriveFolderItemLabels { hidden = true },
                parents = new GoogleDriveParentReference[] { new GoogleDriveParentReference { id = parent } }
            };

            return await m_oauth.PostAndGetJSONDataAsync<GoogleDriveFolderItem>(
                WebApi.GoogleDrive.CreateFolderUrl(m_teamDriveID), folder, "POST", cancelToken);
        }
    }
}

