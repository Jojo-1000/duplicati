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
using Duplicati.Library.Common.IO;
using Duplicati.Library.Interface;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Duplicati.UnitTest
{
    public class SizeOmittingBackend : IBackend
    {
        static SizeOmittingBackend() { WrappedBackend = "file"; }

        public static string WrappedBackend { get; set; }

        private IBackend m_backend;
        public SizeOmittingBackend()
        {
        }

        // ReSharper disable once UnusedMember.Global
        public SizeOmittingBackend(string url, Dictionary<string, string> options)
        {
            var u = new Library.Utility.Uri(url).SetScheme(WrappedBackend).ToString();
            m_backend = Library.DynamicLoader.BackendLoader.GetBackend(u, options);
        }

        #region IBackend implementation
        public async Task<IList<IFileEntry>> ListAsync(CancellationToken cancelToken)
        {
            return
                (from n in (await m_backend.ListAsync(cancelToken))
                where !n.IsFolder
                select (IFileEntry)new FileEntry(n.Name)).ToList();
        }

        public Task PutAsync(string remotename, Stream source, CancellationToken cancelToken)
        {
            return m_backend.PutAsync(remotename, source, cancelToken);
        }

        public Task GetAsync(string remotename, Stream destination, CancellationToken cancelToken)
        {
            return m_backend.GetAsync(remotename, destination, cancelToken);
        }

        public Task DeleteAsync(string remotename, CancellationToken cancelToken)
        {
            return m_backend.DeleteAsync(remotename, cancelToken);
        }

        public Task TestAsync(CancellationToken cancelToken)
        {
            return m_backend.TestAsync(cancelToken);
        }

        public Task CreateFolderAsync(CancellationToken cancelToken)
        {
            return m_backend.CreateFolderAsync(cancelToken);
        }
        public bool SupportsStreaming => true;
        public string[] DNSName
        {
            get
            {
                return m_backend.DNSName;
            }
        }
        public string DisplayName
        {
            get
            {
                return "Size Omitting Backend";
            }
        }
        public string ProtocolKey
        {
            get
            {
                return "omitsize";
            }
        }
        public IList<ICommandLineArgument> SupportedCommands
        {
            get
            {
                if (m_backend == null)
                    try { return Duplicati.Library.DynamicLoader.BackendLoader.GetSupportedCommands(WrappedBackend + "://"); }
                catch { }

                return m_backend.SupportedCommands;
            }
        }
        public string Description
        {
            get
            {
                return "A testing backend that does not return size information";
            }
        }

        #endregion
        #region IDisposable implementation
        public void Dispose()
        {
            if (m_backend != null)
                try { m_backend.Dispose(); }
            finally { m_backend = null; }
        }
        #endregion
    }
}

