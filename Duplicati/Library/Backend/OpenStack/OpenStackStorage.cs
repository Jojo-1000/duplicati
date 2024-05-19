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

using System;
using System.Collections.Generic;
using Duplicati.Library.Interface;
using System.Linq;
using System.IO;
using Duplicati.Library.Utility;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using Duplicati.Library.Strings;
using System.Net;
using System.Text;
using Duplicati.Library.Common.IO;
using System.Threading;
using System.Threading.Tasks;
using System.Net.Http;
using System.Net.Http.Headers;

namespace Duplicati.Library.Backend.OpenStack
{
    public class OpenStackStorage : IBackend, IBackendPagination
    {
        private const string DOMAINNAME_OPTION = "openstack-domain-name";
        private const string USERNAME_OPTION = "auth-username";
        private const string PASSWORD_OPTION = "auth-password";
        private const string TENANTNAME_OPTION = "openstack-tenant-name";
        private const string AUTHURI_OPTION = "openstack-authuri";
        private const string VERSION_OPTION = "openstack-version";
        private const string APIKEY_OPTION = "openstack-apikey";
        private const string REGION_OPTION = "openstack-region";

        private const int PAGE_LIMIT = 500;


        private readonly string m_container;
        private readonly string m_prefix;

        private readonly string m_domainName;
        private readonly string m_username;
        private readonly string m_password;
        private readonly string m_authUri;
        private readonly string m_version;
        private readonly string m_tenantName;
        private readonly string m_apikey;
        private readonly string m_region;

        protected string m_simplestorageendpoint;

        private readonly WebHelper m_helper;
        private OpenStackAuthResponse.TokenClass m_accessToken;

        public static readonly KeyValuePair<string, string>[] KNOWN_OPENSTACK_PROVIDERS = {
            new KeyValuePair<string, string>("Rackspace US", "https://identity.api.rackspacecloud.com/v2.0"),
            new KeyValuePair<string, string>("Rackspace UK", "https://lon.identity.api.rackspacecloud.com/v2.0"),
            new KeyValuePair<string, string>("OVH Cloud Storage", "https://auth.cloud.ovh.net/v3"),
            new KeyValuePair<string, string>("Selectel Cloud Storage", "https://auth.selcdn.ru"),
            new KeyValuePair<string, string>("Memset Cloud Storage", "https://auth.storage.memset.com"),
            new KeyValuePair<string, string>("Infomaniak Swiss Backup cluster 1", "https://swiss-backup.infomaniak.com/identity/v3"),
            new KeyValuePair<string, string>("Infomaniak Swiss Backup cluster 2", "https://swiss-backup02.infomaniak.com/identity/v3"),
            new KeyValuePair<string, string>("Infomaniak Swiss Backup cluster 3", "https://swiss-backup03.infomaniak.com/identity/v3"),
            new KeyValuePair<string, string>("Infomaniak Public Cloud 1", "https://api.pub1.infomaniak.cloud/identity/v3"),
            new KeyValuePair<string, string>("Catalyst Cloud - nz-hlz-1 (NZ)", "https://api.nz-hlz-1.catalystcloud.io:5000/v3"),
            new KeyValuePair<string, string>("Catalyst Cloud - nz-por-1 (NZ)", "https://api.nz-por-1.catalystcloud.io:5000/v3"),
        };

        public static readonly KeyValuePair<string, string>[] OPENSTACK_VERSIONS = {
            new KeyValuePair<string, string>("v2.0", "v2"),
            new KeyValuePair<string, string>("v3", "v3"),
        };


        private class Keystone3AuthRequest
        {
            public class AuthContainer
            {
                public Identity identity { get; set; }
                public Scope scope { get; set; }
            }

            public class Identity
            {
                [JsonProperty(ItemConverterType = typeof(StringEnumConverter))]
                public IdentityMethods[] methods { get; set; }

                [JsonProperty("password", NullValueHandling = NullValueHandling.Ignore)]
                public PasswordBasedRequest PasswordCredentials { get; set; }

                public Identity()
                {
                    this.methods = new[] { IdentityMethods.password };
                }
            }

            public class Scope
            {
                public Project project;
            }

            public enum IdentityMethods
            {
                password,
            }

            public class PasswordBasedRequest
            {
                public UserCredentials user { get; set; }
            }

            public class UserCredentials
            {
                public Domain domain { get; set; }
                public string name { get; set; }
                public string password { get; set; }

                public UserCredentials()
                {
                }
                public UserCredentials(Domain domain, string name, string password)
                {
                    this.domain = domain;
                    this.name = name;
                    this.password = password;
                }

            }

            public class Domain
            {
                public string name { get; set; }

                public Domain(string name)
                {
                    this.name = name;
                }
            }

            public class Project
            {
                public Domain domain { get; set; }
                public string name { get; set; }

                public Project(Domain domain, string name)
                {
                    this.domain = domain;
                    this.name = name;
                }
            }

            public AuthContainer auth { get; set; }

            public Keystone3AuthRequest(string domain_name, string username, string password, string project_name)
            {
                Domain domain = new Domain(domain_name);

                this.auth = new AuthContainer();
                this.auth.identity = new Identity();
                this.auth.identity.PasswordCredentials = new PasswordBasedRequest();
                this.auth.identity.PasswordCredentials.user = new UserCredentials(domain, username, password);
                this.auth.scope = new Scope();
                this.auth.scope.project = new Project(domain, project_name);
            }
        }

        private class OpenStackAuthRequest
        {
            public class AuthContainer
            {
                [JsonProperty("RAX-KSKEY:apiKeyCredentials", NullValueHandling = NullValueHandling.Ignore)]
                public ApiKeyBasedRequest ApiCredentials { get; set; }

                [JsonProperty("passwordCredentials", NullValueHandling = NullValueHandling.Ignore)]
                public PasswordBasedRequest PasswordCredentials { get; set; }

                [JsonProperty("tenantName", NullValueHandling = NullValueHandling.Ignore)]
                public string TenantName { get; set; }

                [JsonProperty("token", NullValueHandling = NullValueHandling.Ignore)]
                public TokenBasedRequest Token { get; set; }

            }

            public class ApiKeyBasedRequest
            {
                public string username { get; set; }
                public string apiKey { get; set; }
            }

            public class PasswordBasedRequest
            {
                public string username { get; set; }
                public string password { get; set; }
                public string tenantName { get; set; }
            }

            public class TokenBasedRequest
            {
                public string id { get; set; }
            }


            public AuthContainer auth { get; set; }

            public OpenStackAuthRequest(string tenantname, string username, string password, string apikey)
            {
                this.auth = new AuthContainer();
                this.auth.TenantName = tenantname;

                if (string.IsNullOrEmpty(apikey))
                {
                    this.auth.PasswordCredentials = new PasswordBasedRequest
                    {
                        username = username,
                        password = password,
                    };
                }
                else
                {
                    this.auth.ApiCredentials = new ApiKeyBasedRequest
                    {
                        username = username,
                        apiKey = apikey
                    };
                }

            }
        }

        private class Keystone3AuthResponse
        {
            public TokenClass token { get; set; }

            public class EndpointItem
            {
                // 'interface' is a reserved keyword, so we need this decorator to map it
                [JsonProperty(PropertyName = "interface")]
                public string interface_name { get; set; }
                public string region { get; set; }
                public string url { get; set; }
            }

            public class CatalogItem
            {
                public EndpointItem[] endpoints { get; set; }
                public string name { get; set; }
                public string type { get; set; }
            }
            public class TokenClass
            {
                public CatalogItem[] catalog { get; set; }
                public DateTime? expires_at { get; set; }
            }
        }

        private class OpenStackAuthResponse
        {
            public AccessClass access { get; set; }

            public class TokenClass
            {
                public string id { get; set; }
                public DateTime? expires { get; set; }
            }

            public class EndpointItem
            {
                public string region { get; set; }
                public string tenantId { get; set; }
                public string publicURL { get; set; }
                public string internalURL { get; set; }
            }

            public class ServiceItem
            {
                public string name { get; set; }
                public string type { get; set; }
                public EndpointItem[] endpoints { get; set; }
            }

            public class AccessClass
            {
                public TokenClass token { get; set; }
                public ServiceItem[] serviceCatalog { get; set; }
            }

        }

        private class OpenStackStorageItem
        {
            public string name { get; set; }
            public DateTime? last_modified { get; set; }
            public long? bytes { get; set; }
            public string content_type { get; set; }
            public string subdir { get; set; }
        }

        private class WebHelper : JSONWebHelper
        {
            private readonly OpenStackStorage m_parent;

            public WebHelper(OpenStackStorage parent) { m_parent = parent; }

            public override async Task<HttpRequestMessage> CreateRequestAsync(string url, string method, CancellationToken cancelToken)
            {
                var req = await base.CreateRequestAsync(url, method, cancelToken);
                req.Headers.Add("X-Auth-Token", m_parent.AccessToken);
                return req;
            }
        }

        public OpenStackStorage()
        {
        }

        public OpenStackStorage(string url, Dictionary<string, string> options)
        {
            var uri = new Utility.Uri(url);

            m_container = uri.Host;
            m_prefix = Util.AppendDirSeparator("/" + uri.Path, "/");

            // For OpenStack we do not use a leading slash
            if (m_prefix.StartsWith("/", StringComparison.Ordinal))
                m_prefix = m_prefix.Substring(1);

            options.TryGetValue(DOMAINNAME_OPTION, out m_domainName);
            options.TryGetValue(USERNAME_OPTION, out m_username);
            options.TryGetValue(PASSWORD_OPTION, out m_password);
            options.TryGetValue(TENANTNAME_OPTION, out m_tenantName);
            options.TryGetValue(AUTHURI_OPTION, out m_authUri);
            options.TryGetValue(VERSION_OPTION, out m_version);
            options.TryGetValue(APIKEY_OPTION, out m_apikey);
            options.TryGetValue(REGION_OPTION, out m_region);

            if (string.IsNullOrWhiteSpace(m_username))
                throw new UserInformationException(Strings.OpenStack.MissingOptionError(USERNAME_OPTION), "OpenStackMissingUsername");
            if (string.IsNullOrWhiteSpace(m_authUri))
                throw new UserInformationException(Strings.OpenStack.MissingOptionError(AUTHURI_OPTION), "OpenStackMissingAuthUri");

            switch (m_version)
            {
                case "v3":
                    if (string.IsNullOrWhiteSpace(m_password))
                        throw new UserInformationException(Strings.OpenStack.MissingOptionError(PASSWORD_OPTION), "OpenStackMissingPassword");
                    if (string.IsNullOrWhiteSpace(m_domainName))
                        throw new UserInformationException(Strings.OpenStack.MissingOptionError(DOMAINNAME_OPTION), "OpenStackMissingDomainName");
                    if (string.IsNullOrWhiteSpace(m_tenantName))
                        throw new UserInformationException(Strings.OpenStack.MissingOptionError(TENANTNAME_OPTION), "OpenStackMissingTenantName");
                    break;
                case "v2":
                default:
                    if (string.IsNullOrWhiteSpace(m_apikey))
                    {
                        if (string.IsNullOrWhiteSpace(m_password))
                            throw new UserInformationException(Strings.OpenStack.MissingOptionError(PASSWORD_OPTION), "OpenStackMissingPassword");
                        if (string.IsNullOrWhiteSpace(m_tenantName))
                            throw new UserInformationException(Strings.OpenStack.MissingOptionError(TENANTNAME_OPTION), "OpenStackMissingTenantName");
                    }
                    break;
            }

            m_helper = new WebHelper(this);
        }

        protected virtual string AccessToken
        {
            get
            {
                if (m_accessToken == null || (m_accessToken.expires.HasValue && (m_accessToken.expires.Value - DateTime.UtcNow).TotalSeconds < 30))
                    GetAuthResponseAsync(CancellationToken.None).Wait();

                return m_accessToken.id;
            }
        }

        private static string JoinUrls(string uri, string fragment)
        {
            fragment = fragment ?? "";
            return uri + (uri.EndsWith("/", StringComparison.Ordinal) ? "" : "/") + (fragment.StartsWith("/", StringComparison.Ordinal) ? fragment.Substring(1) : fragment);
        }
        private static string JoinUrls(string uri, string fragment1, string fragment2)
        {
            return JoinUrls(JoinUrls(uri, fragment1), fragment2);
        }

        private async Task GetAuthResponseAsync(CancellationToken cancelToken)
        {
            switch (this.m_version)
            {
                case "v3":
                    await GetKeystone3AuthResponseAsync(cancelToken);
                    break;
                case "v2":
                default:
                    await GetOpenstackAuthResponseAsync(cancelToken);
                    break;
            }
        }

        private async Task<Keystone3AuthResponse> GetKeystone3AuthResponseAsync(CancellationToken cancelToken)
        {
            var helper = new JSONWebHelper();

            var url = JoinUrls(m_authUri, "auth/tokens");
            var data = new Keystone3AuthRequest(m_domainName, m_username, m_password, m_tenantName);

            Keystone3AuthResponse authResp;
            using (var resp = await helper.GetResponseAsync(url, data, "POST", cancelToken))
            {
                if (!resp.IsSuccessStatusCode)
                {
                    throw new HttpRequestStatusException(resp);
                }
                using (var reader = new StreamReader(await resp.Content.ReadAsStreamAsync()))
                {
                    authResp = Newtonsoft.Json.JsonConvert.DeserializeObject<Keystone3AuthResponse>(
                        await reader.ReadToEndAsync());
                }

                string token = resp.Headers.GetValues("X-Subject-Token").FirstOrDefault();
                this.m_accessToken = new OpenStackAuthResponse.TokenClass();
                this.m_accessToken.id = token;
                this.m_accessToken.expires = authResp.token.expires_at;
            }

            // Grab the endpoint now that we have received it anyway
            var fileservice = authResp.token.catalog.FirstOrDefault(x => string.Equals(x.type, "object-store", StringComparison.OrdinalIgnoreCase));
            if (fileservice == null)
                throw new Exception("No object-store service found, is this service supported by the provider?");

            var endpoint = fileservice.endpoints.FirstOrDefault(
                x => (string.Equals(m_region, x.region) && string.Equals(x.interface_name, "public", StringComparison.OrdinalIgnoreCase)))
                ?? fileservice.endpoints.First();
            m_simplestorageendpoint = endpoint.url;

            return authResp;
        }

        private async Task<OpenStackAuthResponse> GetOpenstackAuthResponseAsync(CancellationToken cancelToken)
        {
            var helper = new JSONWebHelper();

            var resp = await helper.ReadJSONResponseAsync<OpenStackAuthResponse>(JoinUrls(m_authUri, "tokens"),
                new OpenStackAuthRequest(m_tenantName, m_username, m_password, m_apikey),
                "POST",
                cancelToken
            );

            m_accessToken = resp.access.token;

            // Grab the endpoint now that we have received it anyway
            var fileservice = resp.access.serviceCatalog.FirstOrDefault(x => string.Equals(x.type, "object-store", StringComparison.OrdinalIgnoreCase));
            if (fileservice == null)
                throw new Exception("No object-store service found, is this service supported by the provider?");

            var endpoint = fileservice.endpoints.FirstOrDefault(x => string.Equals(m_region, x.region)) ?? fileservice.endpoints.First();

            m_simplestorageendpoint = endpoint.publicURL;

            return resp;
        }

        protected virtual string SimpleStorageEndPoint
        {
            get
            {
                if (m_simplestorageendpoint == null)
                    GetAuthResponseAsync(CancellationToken.None).Wait();

                return m_simplestorageendpoint;
            }
        }

        #region IBackendPagination implementation

        public async IAsyncEnumerable<IFileEntry> ListEnumerableAsync([System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancelToken)
        {
            var plainurl = JoinUrls(SimpleStorageEndPoint, m_container) + string.Format("?format=json&delimiter=/&limit={0}", PAGE_LIMIT);
            if (!string.IsNullOrEmpty(m_prefix))
                plainurl += "&prefix=" + Library.Utility.Uri.UrlEncode(m_prefix);

            var url = plainurl;

            while (true)
            {
                var req = await m_helper.CreateRequestAsync(url, "GET", cancelToken);
                req.Headers.Accept.Clear();
                req.Headers.Accept.ParseAdd("application/json");

                var items = await HandleListExceptions(async () => await m_helper.ReadJSONResponseAsync<OpenStackStorageItem[]>(req, cancelToken));
                foreach (var n in items)
                {
                    var name = n.name;
                    if (name.StartsWith(m_prefix, StringComparison.Ordinal))
                        name = name.Substring(m_prefix.Length);

                    if (n.bytes == null)
                        yield return new FileEntry(name);
                    else if (n.last_modified == null)
                        yield return new FileEntry(name, n.bytes.Value);
                    else
                        yield return new FileEntry(name, n.bytes.Value, n.last_modified.Value, n.last_modified.Value);
                }

                if (items.Length != PAGE_LIMIT)
                    yield break;

                // Prepare next listing entry
                url = plainurl + string.Format("&marker={0}", Library.Utility.Uri.UrlEncode(items.Last().name));
            }
        }

        #endregion
        #region IBackend implementation

        private async Task<T> HandleListExceptions<T>(Func<Task<T>> func)
        {
            try
            {
                return await func();
            }
            catch (HttpRequestStatusException ex)
            {
                if (ex.Response.StatusCode == HttpStatusCode.NotFound)
                    throw new FolderMissingException();
                else
                    throw;
            }
        }

        public Task<IList<IFileEntry>> ListAsync(CancellationToken cancelToken)
            => this.CondensePaginatedListAsync(cancelToken);

        public async Task PutAsync(string remotename, Stream stream, CancellationToken cancelToken)
        {
            var url = JoinUrls(SimpleStorageEndPoint, m_container, Utility.Uri.UrlPathEncode(m_prefix + remotename));
            using (var resp = await m_helper.GetResponseAsync(url, stream, "PUT", cancelToken))
            {
                if (!resp.IsSuccessStatusCode)
                {
                    throw new HttpRequestStatusException(resp);
                }
            }
        }

        public async Task GetAsync(string remotename, Stream destination, CancellationToken cancelToken)
        {
            var url = JoinUrls(SimpleStorageEndPoint, m_container, Utility.Uri.UrlPathEncode(m_prefix + remotename));

            using (var resp = await m_helper.GetResponseWithoutExceptionAsync(url, null, "GET", cancelToken))
            {
                if (resp.StatusCode == HttpStatusCode.NotFound)
                {
                    throw new FileMissingException();
                }
                else if (!resp.IsSuccessStatusCode)
                {
                    throw new HttpRequestStatusException(resp);
                }
                using (var rs = await resp.Content.ReadAsStreamAsync())
                    Library.Utility.Utility.CopyStream(rs, destination);
            }
        }

        public async Task DeleteAsync(string remotename, CancellationToken cancelToken)
        {
            var url = JoinUrls(SimpleStorageEndPoint, m_container, Library.Utility.Uri.UrlPathEncode(m_prefix + remotename));
            await m_helper.ReadJSONResponseAsync<object>(url, null, "DELETE", cancelToken);
        }

        public Task TestAsync(CancellationToken cancelToken)
            => this.TestListAsync(cancelToken);

        public async Task CreateFolderAsync(CancellationToken cancelToken)
        {
            var url = JoinUrls(SimpleStorageEndPoint, m_container);
            using (await m_helper.GetResponseAsync(url, null, "PUT", cancelToken))
            { }
        }

        public string DisplayName
        {
            get
            {
                return Strings.OpenStack.DisplayName;
            }
        }
        public string ProtocolKey
        {
            get
            {
                return "openstack";
            }
        }
        public IList<ICommandLineArgument> SupportedCommands
        {
            get
            {
                var authuris = new StringBuilder();
                foreach (var s in KNOWN_OPENSTACK_PROVIDERS)
                    authuris.AppendLine(string.Format("{0}: {1}", s.Key, s.Value));

                return new List<ICommandLineArgument>(new[] {
                    new CommandLineArgument(DOMAINNAME_OPTION, CommandLineArgument.ArgumentType.String, Strings.OpenStack.DomainnameOptionShort, Strings.OpenStack.DomainnameOptionLong),
                    new CommandLineArgument(USERNAME_OPTION, CommandLineArgument.ArgumentType.String, Strings.OpenStack.UsernameOptionShort, Strings.OpenStack.UsernameOptionLong),
                    new CommandLineArgument(PASSWORD_OPTION, CommandLineArgument.ArgumentType.Password, Strings.OpenStack.PasswordOptionShort, Strings.OpenStack.PasswordOptionLong(TENANTNAME_OPTION)),
                    new CommandLineArgument(TENANTNAME_OPTION, CommandLineArgument.ArgumentType.String, Strings.OpenStack.TenantnameOptionShort, Strings.OpenStack.TenantnameOptionLong),
                    new CommandLineArgument(APIKEY_OPTION, CommandLineArgument.ArgumentType.Password, Strings.OpenStack.ApikeyOptionShort, Strings.OpenStack.ApikeyOptionLong),
                    new CommandLineArgument(AUTHURI_OPTION, CommandLineArgument.ArgumentType.String, Strings.OpenStack.AuthuriOptionShort, Strings.OpenStack.AuthuriOptionLong(authuris.ToString())),
                    new CommandLineArgument(VERSION_OPTION, CommandLineArgument.ArgumentType.String, Strings.OpenStack.VersionOptionShort, Strings.OpenStack.VersionOptionLong),
                    new CommandLineArgument(REGION_OPTION, CommandLineArgument.ArgumentType.String, Strings.OpenStack.RegionOptionShort, Strings.OpenStack.RegionOptionLong),
                });
            }
        }
        public string Description
        {
            get
            {
                return Strings.OpenStack.Description;
            }
        }

        public virtual string[] DNSName
        {
            get
            {
                return new string[] {
                    new System.Uri(m_authUri).Host,
                    string.IsNullOrWhiteSpace(m_simplestorageendpoint) ? null : new System.Uri(m_simplestorageendpoint).Host
                };
            }
        }

        public bool SupportsStreaming => true;

        #endregion
        #region IDisposable implementation
        public void Dispose()
        {
        }
        #endregion
    }
}

