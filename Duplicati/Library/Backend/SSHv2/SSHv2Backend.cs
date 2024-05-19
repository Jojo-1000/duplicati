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


using Duplicati.Library.Common.IO;
using Duplicati.Library.Interface;
using Renci.SshNet;
using Renci.SshNet.Common;
using Renci.SshNet.Sftp;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;

namespace Duplicati.Library.Backend
{
    public class SSHv2 : IBackend, IBackendPagination, IRenameEnabledBackend
    {
        private const string SSH_KEYFILE_OPTION = "ssh-keyfile";
        private const string SSH_KEYFILE_INLINE = "ssh-key";
        private const string SSH_FINGERPRINT_OPTION = "ssh-fingerprint";
        private const string SSH_FINGERPRINT_ACCEPT_ANY_OPTION = "ssh-accept-any-fingerprints";
        public const string KEYFILE_URI = "sshkey://";
        private const string SSH_TIMEOUT_OPTION = "ssh-operation-timeout";
        private const string SSH_KEEPALIVE_OPTION = "ssh-keepalive";

        readonly Dictionary<string, string> m_options;

        private readonly string m_server;
        private readonly string m_path;
        private readonly string m_username;
        private readonly string m_password;
        private readonly string m_fingerprint;
        private readonly bool m_fingerprintallowall;
        private readonly TimeSpan m_operationtimeout;
        private readonly TimeSpan m_keepaliveinterval;

        private readonly int m_port = 22;

        // TODO: A future release of SSH.NET will have async methods
        private SftpClient m_con;

        public SSHv2()
        {
        }

        public SSHv2(string url, Dictionary<string, string> options)
            : this()
        {
            m_options = options;
            var uri = new Utility.Uri(url);
            uri.RequireHost();

            if (options.ContainsKey("auth-username"))
                m_username = options["auth-username"];
            if (options.ContainsKey("auth-password"))
                m_password = options["auth-password"];
            if (options.ContainsKey(SSH_FINGERPRINT_OPTION))
                m_fingerprint = options[SSH_FINGERPRINT_OPTION];
            if (!string.IsNullOrEmpty(uri.Username))
                m_username = uri.Username;
            if (!string.IsNullOrEmpty(uri.Password))
                m_password = uri.Password;

            m_fingerprintallowall = Utility.Utility.ParseBoolOption(options, SSH_FINGERPRINT_ACCEPT_ANY_OPTION);

            m_path = uri.Path;

            if (!string.IsNullOrWhiteSpace(m_path))
            {
                m_path = Util.AppendDirSeparator(m_path, "/");
            }

            if (!m_path.StartsWith("/", StringComparison.Ordinal))
                m_path = "/" + m_path;

            m_server = uri.Host;

            if (uri.Port > 0)
                m_port = uri.Port;

            string timeoutstr;
            options.TryGetValue(SSH_TIMEOUT_OPTION, out timeoutstr);

            if (!string.IsNullOrWhiteSpace(timeoutstr))
                m_operationtimeout = Library.Utility.Timeparser.ParseTimeSpan(timeoutstr);

            options.TryGetValue(SSH_KEEPALIVE_OPTION, out timeoutstr);

            if (!string.IsNullOrWhiteSpace(timeoutstr))
                m_keepaliveinterval = Library.Utility.Timeparser.ParseTimeSpan(timeoutstr);
        }

        #region IBackend Members

        public Task<IList<IFileEntry>> ListAsync(CancellationToken cancelToken)
            => this.CondensePaginatedListAsync(cancelToken);

        public Task TestAsync(CancellationToken cancelToken)
            => this.TestListAsync(cancelToken);

        public async Task CreateFolderAsync(CancellationToken cancelToken)
        {
            await CreateConnectionAsync(cancelToken);

            // Since the SftpClient.CreateDirectory method does not create all the parent directories
            // as needed, this has to be done manually.
            string partialPath = String.Empty;
            foreach (string part in m_path.Split('/').Where(x => !String.IsNullOrEmpty(x)))
            {
                // Cancel early if requested
                cancelToken.ThrowIfCancellationRequested();

                partialPath += $"/{part}";
                if (this.m_con.Exists(partialPath))
                {
                    if (!this.m_con.GetAttributes(partialPath).IsDirectory)
                    {
                        throw new ArgumentException($"The path {partialPath} already exists and is not a directory.");
                    }
                }
                else
                {
                    this.m_con.CreateDirectory(partialPath);
                }
            }
        }

        public string DisplayName => Strings.SSHv2Backend.DisplayName;

        public string ProtocolKey => "ssh";

        public async Task PutAsync(string remotename, System.IO.Stream stream, CancellationToken cancelToken)
        {
            await CreateConnectionAsync(cancelToken);
            ChangeDirectory(m_path);

            // TODO: Use async method once available
            var ares = m_con.BeginUploadFile(stream, remotename, null, null);
            while (!ares.IsCompleted)
            {
                // This probably leaves the operation running in the background if canceled, but a quick stop is probably desired
                await Task.Delay(100, cancelToken);
            }
            m_con.EndUploadFile(ares);
        }

        public async Task GetAsync(string remotename, System.IO.Stream destination, CancellationToken cancelToken)
        {
            try
            {
                await CreateConnectionAsync(cancelToken);
                ChangeDirectory(m_path);

                // TODO: Use async method once available
                var ares = m_con.BeginDownloadFile(remotename, destination, null, null);
                while (!ares.IsCompleted)
                {
                    // This probably leaves the operation running in the background if canceled, but a quick stop is probably desired
                    await Task.Delay(100, cancelToken);
                }
                m_con.EndDownloadFile(ares);
            }
            catch (SftpPathNotFoundException ex)
            {
                throw new FileMissingException(ex);
            }
        }

        public async Task DeleteAsync(string remotename, CancellationToken cancelToken)
        {
            try
            {
                await CreateConnectionAsync(cancelToken);
                ChangeDirectory(m_path);
                m_con.DeleteFile(remotename);
            }
            catch (SftpPathNotFoundException ex)
            {
                throw new FileMissingException(ex);
            }
        }

        public IList<ICommandLineArgument> SupportedCommands
        {
            get
            {
                return new List<ICommandLineArgument>(new ICommandLineArgument[]
                {
                    new CommandLineArgument("auth-password", CommandLineArgument.ArgumentType.Password,
                        Strings.SSHv2Backend.DescriptionAuthPasswordShort,
                        Strings.SSHv2Backend.DescriptionAuthPasswordLong),
                    new CommandLineArgument("auth-username", CommandLineArgument.ArgumentType.String,
                        Strings.SSHv2Backend.DescriptionAuthUsernameShort,
                        Strings.SSHv2Backend.DescriptionAuthUsernameLong),
                    new CommandLineArgument(SSH_FINGERPRINT_OPTION, CommandLineArgument.ArgumentType.String,
                        Strings.SSHv2Backend.DescriptionFingerprintShort,
                        Strings.SSHv2Backend.DescriptionFingerprintLong),
                    new CommandLineArgument(SSH_FINGERPRINT_ACCEPT_ANY_OPTION, CommandLineArgument.ArgumentType.Boolean,
                        Strings.SSHv2Backend.DescriptionAnyFingerprintShort,
                        Strings.SSHv2Backend.DescriptionAnyFingerprintLong),
                    new CommandLineArgument(SSH_KEYFILE_OPTION, CommandLineArgument.ArgumentType.Path,
                        Strings.SSHv2Backend.DescriptionSshkeyfileShort,
                        Strings.SSHv2Backend.DescriptionSshkeyfileLong),
                    new CommandLineArgument(SSH_KEYFILE_INLINE, CommandLineArgument.ArgumentType.Password,
                        Strings.SSHv2Backend.DescriptionSshkeyShort,
                        Strings.SSHv2Backend.DescriptionSshkeyLong(KEYFILE_URI)),
                    new CommandLineArgument(SSH_TIMEOUT_OPTION, CommandLineArgument.ArgumentType.Timespan,
                        Strings.SSHv2Backend.DescriptionSshtimeoutShort, Strings.SSHv2Backend.DescriptionSshtimeoutLong,
                        "0"),
                    new CommandLineArgument(SSH_KEEPALIVE_OPTION, CommandLineArgument.ArgumentType.Timespan,
                        Strings.SSHv2Backend.DescriptionSshkeepaliveShort,
                        Strings.SSHv2Backend.DescriptionSshkeepaliveLong, "0"),
                });
            }
        }

        public string Description => Strings.SSHv2Backend.Description;

        #endregion

        #region IDisposable Members

        public void Dispose()
        {
            if (m_con != null)
            {
                try
                {
                    m_con.Dispose();
                }
                catch (System.Net.Sockets.SocketException)
                {
                    //If the operating system sometimes close socket before disposal of connection following exception is thrown
                    //System.Net.Sockets.SocketException (0x80004005): An existing connection was forcibly closed by the remote host 
                }
                finally
                {
                    m_con = null;
                }
            }
        }

        #endregion

        #region IBackendPagination Implementation

        public async IAsyncEnumerable<IFileEntry> ListEnumerableAsync([System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancelToken)
        {
            string path = ".";

            await CreateConnectionAsync(cancelToken);
            ChangeDirectory(m_path);

            // TODO: Use async method once available
            var ares = m_con.BeginListDirectory(path, null, null);
            while (!ares.IsCompleted)
            {
                // This probably leaves the operation running in the background if canceled, but a quick stop is probably desired
                await Task.Delay(100, cancelToken);
            }
            IEnumerable<ISftpFile> files = m_con.EndListDirectory(ares);
            foreach (var ls in files)
            {
                if (ls.Name.ToString() == "." || ls.Name.ToString() == "..") continue;
                yield return new FileEntry(ls.Name, ls.Length,
                    ls.LastAccessTime, ls.LastWriteTime)
                { IsFolder = ls.Attributes.IsDirectory };
            }
        }

        #endregion

        #region IRenameEnabledBackend Implementation

        public async Task RenameAsync(string oldname, string newname, CancellationToken cancelToken)
        {
            await CreateConnectionAsync(cancelToken);
            ChangeDirectory(m_path);
            cancelToken.ThrowIfCancellationRequested();
            m_con.RenameFile(oldname, newname);
        }

        #endregion

        #region Implementation

        private async Task CreateConnectionAsync(CancellationToken cancelToken)
        {
            if (m_con != null && m_con.IsConnected)
                return;

            if (m_con != null && !m_con.IsConnected)
            {
                await this.TryConnectAsync(m_con, cancelToken).ConfigureAwait(false);
                return;
            }

            SftpClient con;

            m_options.TryGetValue(SSH_KEYFILE_OPTION, out var keyFile);
            if (string.IsNullOrWhiteSpace(keyFile))
            {
                m_options.TryGetValue(SSH_KEYFILE_INLINE, out keyFile);
            }

            if (!string.IsNullOrWhiteSpace(keyFile))
            {
                con = new SftpClient(m_server, m_port, m_username, ValidateKeyFile(keyFile, m_password));
            }
            else
            {
                con = new SftpClient(m_server, m_port, m_username, m_password ?? string.Empty);
            }

            con.HostKeyReceived += (sender, e) =>
            {
                e.CanTrust = false;

                if (m_fingerprintallowall)
                {
                    e.CanTrust = true;
                    return;
                }

                var hostFingerprint =
                    $"{e.HostKeyName} {e.KeyLength.ToString()} {BitConverter.ToString(e.FingerPrint).Replace('-', ':')}";

                if (string.IsNullOrEmpty(m_fingerprint))
                    throw new Library.Utility.HostKeyException(
                        Strings.SSHv2Backend.FingerprintNotSpecifiedManagedError(
                            hostFingerprint.ToLower(CultureInfo.InvariantCulture), SSH_FINGERPRINT_OPTION,
                            SSH_FINGERPRINT_ACCEPT_ANY_OPTION),
                        hostFingerprint, m_fingerprint);

                if (!string.Equals(hostFingerprint, m_fingerprint, StringComparison.OrdinalIgnoreCase))
                    throw new Library.Utility.HostKeyException(
                        Strings.SSHv2Backend.FingerprintNotMatchManagedError(
                            hostFingerprint.ToLower(CultureInfo.InvariantCulture)),
                        hostFingerprint, m_fingerprint);

                e.CanTrust = true;
            };

            if (m_operationtimeout.Ticks != 0)
                con.OperationTimeout = m_operationtimeout;
            if (m_keepaliveinterval.Ticks != 0)
                con.KeepAliveInterval = m_keepaliveinterval;

            await this.TryConnectAsync(con, cancelToken).ConfigureAwait(false);

            m_con = con;
        }

        private Task TryConnectAsync(SftpClient client, CancellationToken cancelToken)
        {
            // TODO: Use ConnectAsync once available
            client.Connect();
            return Task.CompletedTask;
        }

        private void ChangeDirectory(string path)
        {
            if (string.IsNullOrEmpty(path))
                return;

            var workingDir = Util.AppendDirSeparator(m_con.WorkingDirectory, "/");
            if (workingDir == path)
                return;

            try
            {
                m_con.ChangeDirectory(path);
            }
            catch (Exception ex)
            {
                throw new Interface.FolderMissingException(
                    Strings.SSHv2Backend.FolderNotFoundManagedError(path, ex.Message), ex);
            }
        }

        private static Renci.SshNet.PrivateKeyFile ValidateKeyFile(string filename, string password)
        {
            try
            {
                if (!filename.StartsWith(KEYFILE_URI, StringComparison.OrdinalIgnoreCase))
                {
                    return String.IsNullOrEmpty(password)
                        ? new PrivateKeyFile(filename)
                        : new PrivateKeyFile(filename, password);
                }

                using (var ms = new System.IO.MemoryStream())
                using (var sr = new System.IO.StreamWriter(ms))
                {
                    sr.Write(Utility.Uri.UrlDecode(filename.Substring(KEYFILE_URI.Length)));
                    sr.Flush();

                    ms.Position = 0;

                    return String.IsNullOrEmpty(password) ? new PrivateKeyFile(ms) : new PrivateKeyFile(ms, password);
                }
            }
            catch (Exception ex)
            {
                throw new UserInformationException(
                    "Failed to parse the keyfile, check the key format and passphrase. " +
                    $"Error message was {ex.Message}",
                    "SSHFailedToParseKeyFile", ex);
            }
        }

        #endregion

        internal SftpClient Client
        {
            get { return m_con; }
        }

        public string[] DNSName
        {
            get { return new[] { m_server }; }
        }

        public bool SupportsStreaming => true;
    }
}