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
using Duplicati.Library.Utility;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;

namespace Duplicati.Library.Backend
{
    public class TahoeBackend : IBackend, IBackendPagination
    {
        private readonly string m_url;
        private readonly bool m_useSSL = false;
        private readonly byte[] m_copybuffer = new byte[Duplicati.Library.Utility.Utility.DEFAULT_BUFFER_SIZE];
        private readonly HttpClient m_client;

        private class TahoeEl
        {
            public string nodetype { get; set; }
            public TahoeNode node { get; set; }
        }

        private class TahoeNode
        {
            public string rw_uri { get; set; }
            public string verify_uri { get; set; }
            public string ro_uri { get; set; }
            public Dictionary<string, TahoeEl> children { get; set; }
            public bool mutable { get; set; }
            public long size { get; set; }
            public TahoeMetadata metadata { get; set; }
        }

        private class TahoeMetadata
        {
            public TahoeStamps tahoe { get; set; }
        }

        private class TahoeStamps
        {
            public double linkmotime { get; set; }
            public double linkcrtime { get; set; }
        }

        private class TahoeElConverter : JsonConverter
        {
            public override bool CanConvert(Type objectType)
            {
                return objectType == typeof(TahoeEl);
            }

            public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
            {
                var array = JArray.Load(reader);
                string nodetype = null;
                TahoeNode node = null;
                foreach (var token in array.Children())
                    if (token.Type == JTokenType.String)
                        nodetype = token.ToString();
                    else if (token.Type == JTokenType.Object)
                        node = token.ToObject<TahoeNode>(serializer);

                return new TahoeEl() { nodetype = nodetype, node = node };
            }

            public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
            {
                throw new NotImplementedException();
            }
        }


        public TahoeBackend()
        {
        }

        public TahoeBackend(string url, Dictionary<string, string> options)
        {
            //Validate URL
            var u = new Utility.Uri(url);
            u.RequireHost();

            if (!u.Path.StartsWith("uri/URI:DIR2:", StringComparison.Ordinal) && !u.Path.StartsWith("uri/URI%3ADIR2%3A", StringComparison.Ordinal))
                throw new UserInformationException(Strings.TahoeBackend.UnrecognizedUriError, "TahoeInvalidUri");

            m_useSSL = Utility.Utility.ParseBoolOption(options, "use-ssl");

            m_url = u.SetScheme(m_useSSL ? "https" : "http").SetQuery(null).SetCredentials(null, null).ToString();
            m_url = Util.AppendDirSeparator(m_url, "/");

            // Disable cookies to prevent unexpected behavior
            m_client = new HttpClient(new HttpClientHandler() { UseCookies = false })
            {
                BaseAddress = new System.Uri(m_url)
            };
            m_client.DefaultRequestHeaders.TryAddWithoutValidation("User-Agent", "Duplicati Tahoe-LAFS Client v" + System.Reflection.Assembly.GetExecutingAssembly().GetName().Version);
            // Disable keep-alive
            m_client.DefaultRequestHeaders.ConnectionClose = true;
        }

        private string CreateRequestUri(string remotename, string queryparams)
        {
            return (Library.Utility.Uri.UrlEncode(remotename, spacevalue: "%20") + (string.IsNullOrEmpty(queryparams) || queryparams.Trim().Length == 0 ? "" : "?" + queryparams));
        }

        #region IBackend Members

        public Task TestAsync(CancellationToken cancelToken)
            => this.TestListAsync(cancelToken);

        public async Task CreateFolderAsync(CancellationToken cancelToken)
        {
            using (await m_client.PostAsync(CreateRequestUri("", "t=mkdir"), null, cancelToken))
            { }
        }

        public string DisplayName
        {
            get { return Strings.TahoeBackend.Displayname; }
        }

        public string ProtocolKey
        {
            get { return "tahoe"; }
        }

        public Task<IList<IFileEntry>> ListAsync(CancellationToken cancelToken)
            => this.CondensePaginatedListAsync(cancelToken);


        public async Task PutAsync(string remotename, System.IO.Stream stream, CancellationToken cancelToken)
        {
            using (var content = new StreamContent(stream))
            {
                try { content.Headers.ContentLength = stream.Length; } catch { }
                // TODO: why not application/octet-stream?
                content.Headers.ContentType = System.Net.Http.Headers.MediaTypeHeaderValue.Parse("application/binary");
                using (var resp = await m_client.PutAsync(CreateRequestUri(remotename, ""), content, cancelToken).ConfigureAwait(false))
                {
                    if (resp.StatusCode == HttpStatusCode.Conflict || resp.StatusCode == HttpStatusCode.NotFound)
                    {
                        throw new FolderMissingException(Strings.TahoeBackend.MissingFolderError(m_url, resp.ReasonPhrase));
                    }
                    else if (!resp.IsSuccessStatusCode)
                    {
                        throw new HttpRequestStatusException(resp);
                    }
                }
            }
        }

        public async Task GetAsync(string remotename, System.IO.Stream destination, CancellationToken cancelToken)
        {
            using (var resp = await m_client.GetAsync(CreateRequestUri(remotename, ""), cancelToken).ConfigureAwait(false))
            {
                if (!resp.IsSuccessStatusCode)
                {
                    throw new HttpRequestStatusException(resp);
                }

                using (var s = await resp.Content.ReadAsStreamAsync().ConfigureAwait(false))
                    Utility.Utility.CopyStream(s, destination, true, m_copybuffer);
            }
        }

        public async Task DeleteAsync(string remotename, CancellationToken cancelToken)
        {
            using (var resp = await m_client.DeleteAsync(CreateRequestUri(remotename, ""), cancelToken))
            {
                if (resp.StatusCode == HttpStatusCode.NotFound)
                {
                    throw new FileMissingException(resp.ReasonPhrase);
                }
                else if (!resp.IsSuccessStatusCode)
                {
                    throw new HttpRequestStatusException(resp);
                }
            }
        }

        public IList<ICommandLineArgument> SupportedCommands
        {
            get
            {
                return new List<ICommandLineArgument>(new ICommandLineArgument[] {
                    new CommandLineArgument("use-ssl", CommandLineArgument.ArgumentType.Boolean, Strings.TahoeBackend.DescriptionUseSSLShort, Strings.TahoeBackend.DescriptionUseSSLLong),
                });
            }
        }

        public string Description
        {
            get { return Strings.TahoeBackend.Description; }
        }

        public string[] DNSName
        {
            get { return new string[] { new System.Uri(m_url).Host }; }
        }

        public bool SupportsStreaming => true;

        #endregion

        #region IDisposable Members

        public void Dispose()
        {
            m_client.Dispose();
        }

        #endregion

        #region IBackendPagination Members

        public async IAsyncEnumerable<IFileEntry> ListEnumerableAsync([System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancelToken)
        {
            TahoeEl data;

            using (var resp = await m_client.GetAsync(CreateRequestUri("", "t=json"), cancelToken).ConfigureAwait(false))
            {
                if (resp.StatusCode == HttpStatusCode.Conflict || resp.StatusCode == HttpStatusCode.NotFound)
                {
                    throw new FolderMissingException(Strings.TahoeBackend.MissingFolderError(m_url, resp.ReasonPhrase));
                }
                else if (!resp.IsSuccessStatusCode)
                {
                    throw new HttpRequestStatusException(resp);
                }
                using (var rs = await resp.Content.ReadAsStreamAsync().ConfigureAwait(false))
                using (var sr = new System.IO.StreamReader(rs))
                using (var jr = new Newtonsoft.Json.JsonTextReader(sr))
                {
                    var jsr = new Newtonsoft.Json.JsonSerializer();
                    jsr.Converters.Add(new TahoeElConverter());
                    data = jsr.Deserialize<TahoeEl>(jr);
                }
            }

            if (data == null || data.node == null || data.nodetype != "dirnode")
                throw new Exception("Invalid folder listing response");

            foreach (var e in data.node.children)
            {
                if (e.Value == null || e.Value.node == null)
                    continue;

                bool isDir = e.Value.nodetype == "dirnode";
                bool isFile = e.Value.nodetype == "filenode";

                if (!isDir && !isFile)
                    continue;

                FileEntry fe = new FileEntry(e.Key);
                fe.IsFolder = isDir;

                if (e.Value.node.metadata != null && e.Value.node.metadata.tahoe != null)
                    fe.LastModification = Library.Utility.Utility.EPOCH + TimeSpan.FromSeconds(e.Value.node.metadata.tahoe.linkmotime);

                if (isFile)
                    fe.Size = e.Value.node.size;

                yield return fe;
            }
        }

        #endregion
    }
}
