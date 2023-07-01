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
using Duplicati.Library.Interface;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Duplicati.UnitTest
{
    public class RandomErrorBackend : IBackend
    {
        static RandomErrorBackend() { WrappedBackend = "file"; }

        private static readonly Random random = new Random(42);

        public static string WrappedBackend { get; set; }

        private IBackend m_backend;
        public RandomErrorBackend()
        {
        }

        // ReSharper disable once UnusedMember.Global
        public RandomErrorBackend(string url, Dictionary<string, string> options)
        {
            var u = new Library.Utility.Uri(url).SetScheme(WrappedBackend).ToString();
            m_backend = Library.DynamicLoader.BackendLoader.GetBackend(u, options);
        }

        private void ThrowErrorRandom()
        {
            if (random.NextDouble() > 0.90)
                throw new Exception("Random upload failure");
        }
        #region IBackend implementation
        public bool SupportsStreaming => true;

        public async Task PutAsync(string remotename, Stream stream, CancellationToken cancelToken)
        {
            var uploadError = random.NextDouble() > 0.9;

            using (var f = new Library.Utility.ProgressReportingStream(stream, x => { if (uploadError && stream.Position > stream.Length / 2) throw new Exception("Random upload failure"); }))
                await m_backend.PutAsync(remotename, f, cancelToken);
            ThrowErrorRandom();
        }
        
        public Task<IList<IFileEntry>> ListAsync(CancellationToken cancelToken)
        {
            return m_backend.ListAsync(cancelToken);
        }
        public async Task GetAsync(string remotename, Stream destination, CancellationToken cancelToken)
        {
            ThrowErrorRandom();
            await m_backend.GetAsync(remotename, destination, cancelToken);
            ThrowErrorRandom();
        }

        public async Task DeleteAsync(string remotename, CancellationToken cancelToken)
        {
            ThrowErrorRandom();
            await m_backend.DeleteAsync(remotename, cancelToken);
            ThrowErrorRandom();
        }

        public Task TestAsync(CancellationToken cancelToken)
        {
            return m_backend.TestAsync(cancelToken);
        }

        public Task CreateFolderAsync(CancellationToken cancelToken)
        {
            return m_backend.CreateFolderAsync(cancelToken);
        }
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
                return "Random Error Backend";
            }
        }
        public string ProtocolKey
        {
            get
            {
                return "randomerror";
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
                return "A testing backend that randomly fails";
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

