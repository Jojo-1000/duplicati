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
using CG.Web.MegaApiClient;
using Duplicati.Library.Common.IO;
using Duplicati.Library.Interface;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using OtpNet;

namespace Duplicati.Library.Backend.Mega
{
    // ReSharper disable once UnusedMember.Global
    // This class is instantiated dynamically in the BackendLoader.
    public class MegaBackend : IBackend, IRenameEnabledBackend
    {
        private readonly string m_username = null;
        private readonly string m_password = null;
        private readonly string m_twoFactorKey = null;
        private Dictionary<string, List<INode>> m_filecache;
        private INode m_currentFolder = null;
        private readonly string m_prefix = null;

        private MegaApiClient m_client;

        public MegaBackend()
        {
        }

        private MegaApiClient Client
        {
            get
            {
                if (m_client == null)
                {
                    var cl = new MegaApiClient();
                    if (m_twoFactorKey == null)
                        cl.Login(m_username, m_password);
                    else
                    {
                        var totp = new Totp(Base32Encoding.ToBytes(m_twoFactorKey)).ComputeTotp();
                        cl.Login(m_username, m_password, totp);
                    }
                    m_client = cl;
                }

                return m_client;
            }
        }

        public MegaBackend(string url, Dictionary<string, string> options)
        {
            var uri = new Utility.Uri(url);

            if (options.ContainsKey("auth-username"))
                m_username = options["auth-username"];
            if (options.ContainsKey("auth-password"))
                m_password = options["auth-password"];
            if (options.ContainsKey("auth-two-factor-key"))
                m_twoFactorKey = options["auth-two-factor-key"];

            if (!string.IsNullOrEmpty(uri.Username))
                m_username = uri.Username;
            if (!string.IsNullOrEmpty(uri.Password))
                m_password = uri.Password;

            if (string.IsNullOrEmpty(m_username))
                throw new UserInformationException(Strings.MegaBackend.NoUsernameError, "MegaNoUsername");
            if (string.IsNullOrEmpty(m_password))
                throw new UserInformationException(Strings.MegaBackend.NoPasswordError, "MegaNoPassword");

            m_prefix = uri.HostAndPath ?? "";
        }

        private async Task GetCurrentFolderAsync(CancellationToken cancelToken, bool autocreate = false)
        {
            var parts = m_prefix.Split(new string[] { "/" }, StringSplitOptions.RemoveEmptyEntries);
            var nodes = await Client.GetNodesAsync();

            // Client does not support CancellationToken, so we possibly cancel after each request
            cancelToken.ThrowIfCancellationRequested();

            INode parent = nodes.First(x => x.Type == NodeType.Root);

            foreach (var n in parts)
            {
                var item = nodes.FirstOrDefault(x => x.Name == n && x.Type == NodeType.Directory && x.ParentId == parent.Id);
                if (item == null)
                {
                    if (!autocreate)
                        throw new FolderMissingException();

                    item = await Client.CreateFolderAsync(n, parent);
                    cancelToken.ThrowIfCancellationRequested();
                }

                parent = item;
            }

            m_currentFolder = parent;

            await ResetFileCacheAsync(cancelToken, nodes);
        }

        private INode CurrentFolder
        {
            get
            {
                if (m_currentFolder == null)
                    GetCurrentFolderAsync(CancellationToken.None, false).Wait();

                return m_currentFolder;
            }
        }

        private async Task<INode> GetFileNodeAsync(string name, CancellationToken cancelToken)
        {
            if (m_filecache != null && m_filecache.ContainsKey(name))
                return m_filecache[name].OrderByDescending(x => x.ModificationDate).First();

            await ResetFileCacheAsync(cancelToken);

            if (m_filecache != null && m_filecache.ContainsKey(name))
                return m_filecache[name].OrderByDescending(x => x.ModificationDate).First();

            throw new FileMissingException();
        }

        private async Task ResetFileCacheAsync(CancellationToken cancelToken, IEnumerable<INode> list = null)
        {
            if (m_currentFolder == null)
            {
                await GetCurrentFolderAsync(cancelToken, false);
            }
            else
            {
                m_filecache =
                    (list ?? Client.GetNodes()).Where(x => x.Type == NodeType.File && x.ParentId == CurrentFolder.Id)
                        .GroupBy(x => x.Name, x => x, (k, g) => new KeyValuePair<string, List<INode>>(k, g.ToList()))
                        .ToDictionary(x => x.Key, x => x.Value);
            }
        }

        #region IBackend implementation

        public async Task<IList<IFileEntry>> ListAsync(CancellationToken cancelToken)
        {
            if (m_filecache == null)
                await ResetFileCacheAsync(cancelToken);

            return
                (from n in m_filecache.Values
                 let item = n.OrderByDescending(x => x.ModificationDate).First()
                 select (IFileEntry)new FileEntry(item.Name, item.Size, item.ModificationDate ?? new DateTime(0), item.ModificationDate ?? new DateTime(0))).ToList();
        }

        public async Task PutAsync(string remotename, System.IO.Stream stream, CancellationToken cancelToken)
        {
            try
            {
                if (m_filecache == null)
                    await ResetFileCacheAsync(cancelToken);

                var el = await Client.UploadAsync(stream, remotename, CurrentFolder, new Progress(), null, cancelToken);
                if (m_filecache.ContainsKey(remotename))
                    await DeleteAsync(remotename, cancelToken);

                m_filecache[remotename] = new List<INode>();
                m_filecache[remotename].Add(el);
            }
            catch
            {
                m_filecache = null;
                throw;
            }
        }

        public async Task GetAsync(string remotename, System.IO.Stream destination, CancellationToken cancelToken)
        {
            using (var s = await Client.DownloadAsync(
                await GetFileNodeAsync(remotename, cancelToken),
                cancellationToken: cancelToken))
                Library.Utility.Utility.CopyStream(s, destination);
        }

        public async Task DeleteAsync(string remotename, CancellationToken cancelToken)
        {
            try
            {
                if (m_filecache == null || !m_filecache.ContainsKey(remotename))
                    await ResetFileCacheAsync(cancelToken);

                if (!m_filecache.ContainsKey(remotename))
                    throw new FileMissingException();

                // Client does not support CancellationToken, so we possibly cancel after each request
                cancelToken.ThrowIfCancellationRequested();

                foreach (var n in m_filecache[remotename])
                {
                    await Client.DeleteAsync(n, false);
                    cancelToken.ThrowIfCancellationRequested();
                }

                m_filecache.Remove(remotename);
            }
            catch
            {
                m_filecache = null;
                throw;
            }
        }

        public Task TestAsync(CancellationToken cancelToken)
            => this.TestListAsync(cancelToken);

        public Task CreateFolderAsync(CancellationToken cancelToken)
        {
            return GetCurrentFolderAsync(cancelToken, true);
        }

        public string DisplayName
        {
            get
            {
                return Strings.MegaBackend.DisplayName;
            }
        }

        public string ProtocolKey
        {
            get
            {
                return "mega";
            }
        }

        public IList<ICommandLineArgument> SupportedCommands
        {
            get
            {
                return new List<ICommandLineArgument>(new ICommandLineArgument[] {
                    new CommandLineArgument("auth-password", CommandLineArgument.ArgumentType.Password, Strings.MegaBackend.AuthPasswordDescriptionShort, Strings.MegaBackend.AuthPasswordDescriptionLong),
                    new CommandLineArgument("auth-username", CommandLineArgument.ArgumentType.String, Strings.MegaBackend.AuthUsernameDescriptionShort, Strings.MegaBackend.AuthUsernameDescriptionLong),
                    new CommandLineArgument("auth-two-factor-key", CommandLineArgument.ArgumentType.Password, Strings.MegaBackend.AuthTwoFactorKeyDescriptionShort, Strings.MegaBackend.AuthTwoFactorKeyDescriptionLong),
                });
            }
        }

        public string Description
        {
            get
            {
                return Strings.MegaBackend.Description;
            }
        }

        public string[] DNSName
        {
            get { return null; }
        }

        public bool SupportsStreaming => true;

        #endregion
        #region IRenameEnabledBackend Members

        public async Task RenameAsync(string oldname, string newname, CancellationToken cancelToken)
        {
            try
            {
                if (m_filecache == null || !m_filecache.ContainsKey(oldname))
                    await ResetFileCacheAsync(cancelToken);

                if (!m_filecache.ContainsKey(oldname))
                    throw new FileMissingException();

                // Delete target if exists (consistent with FileBackend rename)
                if (m_filecache.ContainsKey(newname))
                    await DeleteAsync(newname, cancelToken);

                // Client does not support CancellationToken, so we possibly cancel after each request
                cancelToken.ThrowIfCancellationRequested();

                foreach (var n in m_filecache[oldname])
                {
                    await Client.RenameAsync(n, newname);
                    cancelToken.ThrowIfCancellationRequested();
                }

                m_filecache[newname] = m_filecache[oldname];
                m_filecache.Remove(oldname);
            }
            catch
            {
                m_filecache = null;
                throw;
            }
        }

        #endregion

        #region IDisposable implementation

        public void Dispose()
        {
        }

        #endregion

        private class Progress : IProgress<double>
        {
            public void Report(double value)
            {
                // No implementation as we have already wrapped the stream in our own progress reporting stream
            }
        }
    }
}
