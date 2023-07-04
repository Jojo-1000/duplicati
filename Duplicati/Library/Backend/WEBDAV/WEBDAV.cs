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
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;

namespace Duplicati.Library.Backend
{
    public class WEBDAV : IBackend
    {
        private readonly System.Net.NetworkCredential m_userInfo;
        private readonly string m_url;
        private readonly string m_path;
        private readonly string m_sanitizedUrl;
        private readonly string m_reverseProtocolUrl;
        private readonly string m_rawurl;
        private readonly string m_rawurlPort;
        private readonly string m_dnsName;
        private readonly bool m_useIntegratedAuthentication = false;
        private readonly bool m_forceDigestAuthentication = false;
        private readonly bool m_useSSL = false;
        private readonly string m_debugPropfindFile = null;
        private readonly byte[] m_copybuffer = new byte[Duplicati.Library.Utility.Utility.DEFAULT_BUFFER_SIZE];

        /// <summary>
        /// A list of files seen in the last List operation.
        /// It is used to detect a problem with IIS where a file is listed,
        /// but IIS responds 404 because the file mapping is incorrect.
        /// </summary>
        private List<string> m_filenamelist = null;

        // According to the WEBDAV standard, the "allprop" request should return all properties, however this seems to fail on some servers (box.net).
        // I've found this description: http://www.webdav.org/specs/rfc2518.html#METHOD_PROPFIND
        //  "An empty PROPFIND request body MUST be treated as a request for the names and values of all properties."
        //
        //private static readonly byte[] PROPFIND_BODY = System.Text.Encoding.UTF8.GetBytes("<?xml version=\"1.0\"?><D:propfind xmlns:D=\"DAV:\"><D:allprop/></D:propfind>");
        private static readonly byte[] PROPFIND_BODY = new byte[0];

        private readonly HttpClient m_client;

        public WEBDAV()
        {
        }

        public WEBDAV(string url, Dictionary<string, string> options)
        {
            var u = new Utility.Uri(url);
            u.RequireHost();
            m_dnsName = u.Host;

            if (!string.IsNullOrEmpty(u.Username))
            {
                m_userInfo = new System.Net.NetworkCredential();
                m_userInfo.UserName = u.Username;
                if (!string.IsNullOrEmpty(u.Password))
                    m_userInfo.Password = u.Password;
                else if (options.ContainsKey("auth-password"))
                    m_userInfo.Password = options["auth-password"];
            }
            else
            {
                if (options.ContainsKey("auth-username"))
                {
                    m_userInfo = new System.Net.NetworkCredential();
                    m_userInfo.UserName = options["auth-username"];
                    if (options.ContainsKey("auth-password"))
                        m_userInfo.Password = options["auth-password"];
                }
            }

            //Bugfix, see http://connect.microsoft.com/VisualStudio/feedback/details/695227/networkcredential-default-constructor-leaves-domain-null-leading-to-null-object-reference-exceptions-in-framework-code
            if (m_userInfo != null)
                m_userInfo.Domain = "";

            m_useIntegratedAuthentication = Utility.Utility.ParseBoolOption(options, "integrated-authentication");
            m_forceDigestAuthentication = Utility.Utility.ParseBoolOption(options, "force-digest-authentication");
            m_useSSL = Utility.Utility.ParseBoolOption(options, "use-ssl");

            m_url = u.SetScheme(m_useSSL ? "https" : "http").SetCredentials(null, null).SetQuery(null).ToString();
            m_url = Util.AppendDirSeparator(m_url, "/");

            m_path = u.Path;
            if (!m_path.StartsWith("/", StringComparison.Ordinal))
                m_path = "/" + m_path;
            m_path = Util.AppendDirSeparator(m_path, "/");

            m_path = Library.Utility.Uri.UrlDecode(m_path);
            m_rawurl = new Utility.Uri(m_useSSL ? "https" : "http", u.Host, m_path).ToString();

            int port = u.Port;
            if (port <= 0)
                port = m_useSSL ? 443 : 80;

            m_rawurlPort = new Utility.Uri(m_useSSL ? "https" : "http", u.Host, m_path, null, null, null, port).ToString();
            m_sanitizedUrl = new Utility.Uri(m_useSSL ? "https" : "http", u.Host, m_path).ToString();
            m_reverseProtocolUrl = new Utility.Uri(m_useSSL ? "http" : "https", u.Host, m_path).ToString();
            options.TryGetValue("debug-propfind-file", out m_debugPropfindFile);


            bool preAuthenticate = false;
            ICredentials credential;
            if (m_useIntegratedAuthentication)
            {
                credential = null;
            }
            else if (m_forceDigestAuthentication)
            {
                System.Net.CredentialCache cred = new System.Net.CredentialCache();
                cred.Add(new System.Uri(m_url), "Digest", m_userInfo);
                credential = cred;
            }
            else
            {
                credential = m_userInfo;
                //We need this under Mono for some reason,
                // and it appears some servers require this as well
                preAuthenticate = true;
            }

            m_client = new HttpClient(new HttpClientHandler()
            {
                // Disable cookies to prevent unexpected behavior
                UseCookies = false,
                UseDefaultCredentials = m_useIntegratedAuthentication,
                Credentials = credential,
                PreAuthenticate = preAuthenticate
            })
            {
                BaseAddress = new System.Uri(m_url)
            };

            m_client.DefaultRequestHeaders.ConnectionClose = true;
            m_client.DefaultRequestHeaders.TryAddWithoutValidation("User-Agent", "Duplicati WEBDAV Client v" + System.Reflection.Assembly.GetExecutingAssembly().GetName().Version);
        }

        #region IBackend Members

        public string DisplayName
        {
            get { return Strings.WEBDAV.DisplayName; }
        }

        public string ProtocolKey
        {
            get { return "webdav"; }
        }

        public Task<IList<IFileEntry>> ListAsync(CancellationToken cancelToken)
        {
            try
            {
                return ListWithouExceptionCatch(CancellationToken.None);
            }
            catch (HttpRequestStatusException wex)
            {
                if (wex.Response.StatusCode == System.Net.HttpStatusCode.NotFound || wex.Response.StatusCode == System.Net.HttpStatusCode.Conflict)
                    throw new FolderMissingException(Strings.WEBDAV.MissingFolderError(m_path, wex.Message), wex);

                if (wex.Response.StatusCode == System.Net.HttpStatusCode.MethodNotAllowed)
                    throw new UserInformationException(Strings.WEBDAV.MethodNotAllowedError(wex.Response.StatusCode), "WebdavMethodNotAllowed", wex);

                throw;
            }
        }

        private async Task<IList<IFileEntry>> ListWithouExceptionCatch(CancellationToken cancelToken)
        {
            var req = CreateRequest(new HttpMethod("PROPFIND"), "");

            req.Headers.Add("Depth", "1");
            var doc = new System.Xml.XmlDocument();

            using (var body = new ByteArrayContent(PROPFIND_BODY))
            {
                req.Content = body;
                body.Headers.ContentType = System.Net.Http.Headers.MediaTypeHeaderValue.Parse("text/xml");
                body.Headers.ContentLength = PROPFIND_BODY.Length;

                using (var resp = await m_client.SendAsync(req, cancelToken).ConfigureAwait(false))
                {
                    if (!resp.IsSuccessStatusCode)
                    {
                        throw new HttpRequestStatusException(resp);
                    }

                    if (!string.IsNullOrEmpty(m_debugPropfindFile))
                    {
                        using (var rs = await resp.Content.ReadAsStreamAsync().ConfigureAwait(false))
                        using (var fs = new System.IO.FileStream(m_debugPropfindFile, System.IO.FileMode.Create, System.IO.FileAccess.Write, System.IO.FileShare.None))
                            await Utility.Utility.CopyStreamAsync(rs, fs, false, cancelToken, m_copybuffer).ConfigureAwait(false);

                        doc.Load(m_debugPropfindFile);
                    }
                    else
                    {
                        using (var rs = await resp.Content.ReadAsStreamAsync().ConfigureAwait(false))
                            doc.Load(rs);
                    }
                }
            }

            System.Xml.XmlNamespaceManager nm = new System.Xml.XmlNamespaceManager(doc.NameTable);
            nm.AddNamespace("D", "DAV:");

            List<IFileEntry> files = new List<IFileEntry>();
            m_filenamelist = new List<string>();

            foreach (System.Xml.XmlNode n in doc.SelectNodes("D:multistatus/D:response/D:href", nm))
            {
                //IIS uses %20 for spaces and %2B for +
                //Apache uses %20 for spaces and + for +
                string name = Library.Utility.Uri.UrlDecode(n.InnerText.Replace("+", "%2B"));

                string cmp_path;

                //TODO: This list is getting ridiculous, should change to regexps

                if (name.StartsWith(m_url, StringComparison.Ordinal))
                    cmp_path = m_url;
                else if (name.StartsWith(m_rawurl, StringComparison.Ordinal))
                    cmp_path = m_rawurl;
                else if (name.StartsWith(m_rawurlPort, StringComparison.Ordinal))
                    cmp_path = m_rawurlPort;
                else if (name.StartsWith(m_path, StringComparison.Ordinal))
                    cmp_path = m_path;
                else if (name.StartsWith("/" + m_path, StringComparison.Ordinal))
                    cmp_path = "/" + m_path;
                else if (name.StartsWith(m_sanitizedUrl, StringComparison.Ordinal))
                    cmp_path = m_sanitizedUrl;
                else if (name.StartsWith(m_reverseProtocolUrl, StringComparison.Ordinal))
                    cmp_path = m_reverseProtocolUrl;
                else
                    continue;

                if (name.Length <= cmp_path.Length)
                    continue;

                name = name.Substring(cmp_path.Length);

                long size = -1;
                DateTime lastAccess = new DateTime();
                DateTime lastModified = new DateTime();
                bool isCollection = false;

                System.Xml.XmlNode stat = n.ParentNode.SelectSingleNode("D:propstat/D:prop", nm);
                if (stat != null)
                {
                    System.Xml.XmlNode s = stat.SelectSingleNode("D:getcontentlength", nm);
                    if (s != null)
                        size = long.Parse(s.InnerText);
                    s = stat.SelectSingleNode("D:getlastmodified", nm);
                    if (s != null)
                        try
                        {
                            //Not important if this succeeds
                            lastAccess = lastModified = DateTime.Parse(s.InnerText, System.Globalization.CultureInfo.InvariantCulture);
                        }
                        catch { }

                    s = stat.SelectSingleNode("D:iscollection", nm);
                    if (s != null)
                        isCollection = s.InnerText.Trim() == "1";
                    else
                        isCollection = (stat.SelectSingleNode("D:resourcetype/D:collection", nm) != null);
                }

                FileEntry fe = new FileEntry(name, size, lastAccess, lastModified);
                fe.IsFolder = isCollection;
                files.Add(fe);
                m_filenamelist.Add(name);
            }

            return files;
        }

        public async Task PutAsync(string remotename, System.IO.Stream stream, CancellationToken cancelToken)
        {
            var req = CreateRequest(HttpMethod.Put, remotename);
            using (var body = new StreamContent(stream))
            using (var resp = await m_client.SendAsync(req, cancelToken).ConfigureAwait(false))
            {
                if (resp.StatusCode == HttpStatusCode.Conflict || resp.StatusCode == HttpStatusCode.NotFound)
                    throw new FolderMissingException(Strings.WEBDAV.MissingFolderError(m_path, resp.ReasonPhrase));
                else if (!resp.IsSuccessStatusCode)
                    throw new HttpRequestStatusException(resp);
            }
        }

        public async Task GetAsync(string remotename, System.IO.Stream destination, CancellationToken cancelToken)
        {
            var req = CreateRequest(HttpMethod.Get, remotename);

            using (var resp = await m_client.SendAsync(req, cancelToken).ConfigureAwait(false))
            {
                if (resp.StatusCode == HttpStatusCode.Conflict)
                    throw new FileMissingException(Strings.WEBDAV.MissingFolderError(m_path, resp.ReasonPhrase));
                else if (resp.StatusCode == HttpStatusCode.NotFound && m_filenamelist != null && m_filenamelist.Contains(remotename))
                    throw new Exception(Strings.WEBDAV.SeenThenNotFoundError(m_path, remotename, System.IO.Path.GetExtension(remotename), resp.ReasonPhrase));
                else if (!resp.IsSuccessStatusCode)
                    throw new HttpRequestStatusException(resp);

                using (var s = await resp.Content.ReadAsStreamAsync().ConfigureAwait(false))
                    await Utility.Utility.CopyStreamAsync(s, destination, true, cancelToken, m_copybuffer).ConfigureAwait(false);
            }
        }

        public async Task DeleteAsync(string remotename, CancellationToken cancelToken)
        {
            var req = CreateRequest(HttpMethod.Delete, remotename);
            using (var resp = await m_client.SendAsync(req, cancelToken).ConfigureAwait(false))
            {
                if (resp.StatusCode == System.Net.HttpStatusCode.NotFound)
                    throw new FileMissingException();

                if (!resp.IsSuccessStatusCode)
                    throw new HttpRequestStatusException(resp);
            }
        }

        public IList<ICommandLineArgument> SupportedCommands
        {
            get
            {
                return new List<ICommandLineArgument>(new ICommandLineArgument[] {
                    new CommandLineArgument("auth-password", CommandLineArgument.ArgumentType.Password, Strings.WEBDAV.DescriptionAuthPasswordShort, Strings.WEBDAV.DescriptionAuthPasswordLong),
                    new CommandLineArgument("auth-username", CommandLineArgument.ArgumentType.String, Strings.WEBDAV.DescriptionAuthUsernameShort, Strings.WEBDAV.DescriptionAuthUsernameLong),
                    new CommandLineArgument("integrated-authentication", CommandLineArgument.ArgumentType.Boolean, Strings.WEBDAV.DescriptionIntegratedAuthenticationShort, Strings.WEBDAV.DescriptionIntegratedAuthenticationLong),
                    new CommandLineArgument("force-digest-authentication", CommandLineArgument.ArgumentType.Boolean, Strings.WEBDAV.DescriptionForceDigestShort, Strings.WEBDAV.DescriptionForceDigestLong),
                    new CommandLineArgument("use-ssl", CommandLineArgument.ArgumentType.Boolean, Strings.WEBDAV.DescriptionUseSSLShort, Strings.WEBDAV.DescriptionUseSSLLong),
                    new CommandLineArgument("debug-propfind-file", CommandLineArgument.ArgumentType.Path, Strings.WEBDAV.DescriptionDebugPropfindShort, Strings.WEBDAV.DescriptionDebugPropfindLong),
                });
            }
        }

        public string Description
        {
            get { return Strings.WEBDAV.Description; }
        }

        public string[] DNSName
        {
            get { return new string[] { m_dnsName }; }
        }

        public bool SupportsStreaming => true;

        public Task TestAsync(CancellationToken cancelToken)
            => this.TestListAsync(cancelToken);

        public async Task CreateFolderAsync(CancellationToken cancelToken)
        {
            var req = CreateRequest(new HttpMethod(WebRequestMethods.Http.MkCol), "");
            using (var resp = await m_client.SendAsync(req, cancelToken).ConfigureAwait(false))
            {
                if (!resp.IsSuccessStatusCode)
                    throw new HttpRequestStatusException(resp);
            }
        }

        #endregion

        #region IDisposable Members

        public void Dispose()
        {
            m_client.Dispose();
        }

        #endregion

        private HttpRequestMessage CreateRequest(HttpMethod method, string remotename)
        {
            return new HttpRequestMessage(method, Utility.Uri.UrlEncode(remotename, spacevalue: "%20"));
        }
    }
}
