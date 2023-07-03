using Duplicati.Library.Common.IO;
using Duplicati.Library.Interface;
using Duplicati.Library.Utility;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;

namespace Duplicati.Library.Backend.Sia
{
    public class Sia : IBackend
    {
        private const string SIA_PASSWORD = "sia-password";
        private const string SIA_TARGETPATH = "sia-targetpath";
        private const string SIA_REDUNDANCY = "sia-redundancy";

        private readonly string m_apihost;
        private readonly int m_apiport;
        private readonly string m_targetpath;
        private readonly float m_redundancy;
        private readonly string m_authorization;

        private readonly HttpClient m_client;

        // ReSharper disable once UnusedMember.Global
        // This constructor is needed by the BackendLoader.
        public Sia()
        {
        }

        // ReSharper disable once UnusedMember.Global
        // This constructor is needed by the BackendLoader.
        public Sia(string url, Dictionary<string, string> options)
        {
            var uri = new Utility.Uri(url);

            m_apihost = uri.Host;
            m_apiport = uri.Port;
            m_targetpath = uri.Path;

            m_redundancy = 1.5F;
            if (options.ContainsKey(SIA_REDUNDANCY))
                m_redundancy = float.Parse(options[SIA_REDUNDANCY]);

            if (m_apiport <= 0)
                m_apiport = 9980;

            if (options.ContainsKey(SIA_TARGETPATH))
            {
                m_targetpath = options[SIA_TARGETPATH];
            }
            while (m_targetpath.Contains("//"))
                m_targetpath = m_targetpath.Replace("//", "/");
            while (m_targetpath.StartsWith("/", StringComparison.Ordinal))
                m_targetpath = m_targetpath.Substring(1);
            while (m_targetpath.EndsWith("/", StringComparison.Ordinal))
                m_targetpath = m_targetpath.Remove(m_targetpath.Length - 1);

            if (m_targetpath.Length == 0)
                m_targetpath = "backup";

            m_authorization = options.ContainsKey(SIA_PASSWORD) && !string.IsNullOrEmpty(options[SIA_PASSWORD])
                ? "Basic " + System.Convert.ToBase64String(System.Text.Encoding.GetEncoding("ISO-8859-1").GetBytes(":" + options[SIA_PASSWORD]))
                : null;

            // Disable saving cookies, because they can cause unexpected behavior
            m_client = new HttpClient(new HttpClientHandler() { UseCookies = false })
            {
                BaseAddress = new System.Uri("http://" + m_apihost + ":" + m_apiport)
            };
            // Disable keep-alive
            m_client.DefaultRequestHeaders.ConnectionClose = true;
            m_client.DefaultRequestHeaders.TryAddWithoutValidation("User-Agent", string.Format("Sia-Agent (Duplicati SIA client {0})", System.Reflection.Assembly.GetExecutingAssembly().GetName().Version));

            if (m_authorization != null)
            {
                // Manually set Authorization header, since System.Net.NetworkCredential ignores credentials with empty usernames
                m_client.DefaultRequestHeaders.TryAddWithoutValidation("Authorization", m_authorization);
            }
        }

        private async Task<string> getResponseBodyOnErrorAsync(string context, HttpResponseMessage response)
        {
            string body = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
            return string.Format("{0} failed, response: {1}", context, body);
        }

        public class SiaFile
        {
            [JsonProperty("siapath")]
            public string Siapath { get; set; }
            [JsonProperty("available")]
            public bool Available { get; set; }
            [JsonProperty("filesize")]
            public long Filesize { get; set; }
            [JsonProperty("uploadprogress")]
            public float Uploadprogress { get; set; }
            [JsonProperty("redundancy")]
            public float Redundancy { get; set; }
        }

        public class SiaFileList
        {
            [JsonProperty("files")]
            public SiaFile[] Files { get; set; }
        }

        public class SiaDownloadFile
        {
            [JsonProperty("siapath")]
            public string Siapath { get; set; }
            [JsonProperty("destination")]
            public string Destination { get; set; }
            [JsonProperty("filesize")]
            public long Filesize { get; set; }
            [JsonProperty("received")]
            public long Received { get; set; }
            [JsonProperty("starttime")]
            public string Starttime { get; set; }
            [JsonProperty("error")]
            public string Error { get; set; }
        }

        public class SiaDownloadList
        {
            [JsonProperty("downloads")]
            public SiaDownloadFile[] Files { get; set; }
        }

        private async Task<SiaFileList> GetFilesAsync(CancellationToken cancelToken)
        {
            var fl = new SiaFileList();
            string endpoint = "/renter/files";

            try
            {
                using (var resp = await m_client.GetAsync(endpoint, cancelToken).ConfigureAwait(false))
                {
                    if (!resp.IsSuccessStatusCode)
                        throw new Exception(await getResponseBodyOnErrorAsync(endpoint, resp));

                    var serializer = new JsonSerializer();

                    using (var rs = await resp.Content.ReadAsStreamAsync().ConfigureAwait(false))
                    using (var sr = new System.IO.StreamReader(rs))
                    using (var jr = new Newtonsoft.Json.JsonTextReader(sr))
                    {
                        fl = (SiaFileList)serializer.Deserialize(jr, typeof(SiaFileList));
                    }
                }
            }
            catch (Exception ex)
            {
                throw new Exception($"{endpoint} failed with error: {ex.Message}", ex);
            }
            return fl;
        }

        private async Task<bool> IsUploadCompleteAsync(string siafilename, CancellationToken cancelToken)
        {
            SiaFileList fl = await GetFilesAsync(cancelToken).ConfigureAwait(false);
            if (fl.Files == null)
                return false;

            foreach (var f in fl.Files)
            {
                if (f.Siapath == siafilename)
                {
                    if (f.Available == true && f.Redundancy >= m_redundancy /* && f.Uploadprogress >= 100 */ )
                    {
                        return true;
                    }
                }
            }
            return false;
        }

        private async Task<SiaDownloadList> GetDownloadsAsync(CancellationToken cancelToken)
        {
            var fl = new SiaDownloadList();
            string endpoint = "/renter/downloads";

            try
            {
                using (var resp = await m_client.GetAsync(endpoint, cancelToken).ConfigureAwait(false))
                {
                    int code = (int)resp.StatusCode;
                    if (!resp.IsSuccessStatusCode)
                        throw new Exception(await getResponseBodyOnErrorAsync(endpoint, resp).ConfigureAwait(false));

                    var serializer = new JsonSerializer();

                    using (var rs = await resp.Content.ReadAsStreamAsync().ConfigureAwait(false))
                    using (var sr = new System.IO.StreamReader(rs))
                    using (var jr = new Newtonsoft.Json.JsonTextReader(sr))
                    {
                        fl = (SiaDownloadList)serializer.Deserialize(jr, typeof(SiaDownloadList));
                    }
                }
            }
            catch (Exception ex)
            {
                throw new Exception($"{endpoint} failed with error: {ex.Message}", ex);
            }
            return fl;
        }

        private async Task<bool> IsDownloadCompleteAsync(string siafilename, string localname, CancellationToken cancelToken)
        {
            SiaDownloadList fl = await GetDownloadsAsync(cancelToken);
            if (fl.Files == null)
                return false;

            foreach (var f in fl.Files)
            {
                if (f.Siapath == siafilename)
                {
                    if (f.Error != "")
                    {
                        throw new Exception("failed to download " + siafilename + "err: " + f.Error);
                    }
                    if (f.Filesize == f.Received)
                    {
                        try
                        {
                            // Sia seems to keep the file open/locked for a while, make sure we can open it
                            System.IO.FileStream fs = new System.IO.FileStream(localname, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.ReadWrite);
                            fs.Close();
                        }
                        catch (System.IO.IOException)
                        {
                            return false;
                        }
                        return true;
                    }
                }
            }
            return false;
        }

        #region IBackend Members

        public Task TestAsync(CancellationToken cancelToken)
            => this.TestListAsync(cancelToken);

        public Task CreateFolderAsync(CancellationToken cancelToken)
        {
            // Dummy method, Sia doesn't have folders
            return Task.CompletedTask;
        }

        public string DisplayName
        {
            get { return Strings.Sia.DisplayName; }
        }

        public string ProtocolKey
        {
            get { return "sia"; }
        }

        public async Task<IList<IFileEntry>> ListAsync(CancellationToken cancelToken)
        {
            SiaFileList fl;
            try
            {
                fl = await GetFilesAsync(cancelToken).ConfigureAwait(false);
            }
            catch (HttpRequestException wex)
            {
                throw new Exception("failed to call /renter/files " + wex.Message);
            }

            if (fl.Files != null)
            {
                return (from f in fl.Files
                            // Sia returns a complete file list, but we're only interested in files that are in our target path
                        where f.Siapath.StartsWith(m_targetpath, StringComparison.Ordinal)
                        select new FileEntry(f.Siapath.Substring(m_targetpath.Length + 1))
                        {
                            Size = f.Filesize,
                            IsFolder = false
                        } as IFileEntry).ToList();
            }
            return new List<IFileEntry>();
        }

        public async Task PutAsync(string remotename, System.IO.Stream source, CancellationToken cancelToken)
        {
            string siafile = m_targetpath + "/" + remotename;
            string filename = (source as FauxStream).Filename;

            string endpoint = string.Format("/renter/upload/{0}/{1}?source={2}",
                m_targetpath,
                Utility.Uri.UrlEncode(remotename).Replace("+", "%20"),
                Utility.Uri.UrlEncode(filename).Replace("+", "%20")
            );

            using (var resp = await m_client.PostAsync(endpoint, null, cancelToken).ConfigureAwait(false))
            {
                if (!resp.IsSuccessStatusCode)
                    throw new Exception(await getResponseBodyOnErrorAsync(endpoint, resp).ConfigureAwait(false));

                while (!await IsUploadCompleteAsync(siafile, cancelToken).ConfigureAwait(false))
                {
                    await Task.Delay(5000, cancelToken).ConfigureAwait(false);
                }
            }
        }

        public async Task GetAsync(string remotename, System.IO.Stream destination, CancellationToken cancelToken)
        {
            string siafile = m_targetpath + "/" + remotename;
            string localname = (destination as FauxStream).Filename;
            string tmpfilename = localname + ".tmp";

            string endpoint = string.Format("/renter/download/{0}/{1}?destination={2}",
                m_targetpath,
                Library.Utility.Uri.UrlEncode(remotename, spacevalue: "%20"),
                Library.Utility.Uri.UrlEncode(tmpfilename, spacevalue: "%20")
            );

            using (var resp = await m_client.GetAsync(endpoint, cancelToken).ConfigureAwait(false))
            {
                if (resp.StatusCode == HttpStatusCode.NotFound)
                    throw new FileMissingException(resp.ReasonPhrase);
                else if (!resp.IsSuccessStatusCode)
                    throw new Exception(await getResponseBodyOnErrorAsync(endpoint, resp).ConfigureAwait(false));

                while (!await IsDownloadCompleteAsync(siafile, localname, cancelToken))
                {
                    await Task.Delay(5000, cancelToken).ConfigureAwait(false);
                }

                await Task.Run(() =>
                {
                    System.IO.File.Copy(tmpfilename, localname, true);
                    try
                    {
                        System.IO.File.Delete(tmpfilename);
                    }
                    catch { }
                }).ConfigureAwait(false);
            }
        }

        public async Task DeleteAsync(string remotename, CancellationToken cancelToken)
        {
            string endpoint = string.Format("/renter/delete/{0}/{1}",
                m_targetpath,
                Library.Utility.Uri.UrlEncode(remotename, spacevalue: "%20")
            );

            using (var resp = await m_client.PostAsync(endpoint, null, cancelToken).ConfigureAwait(false))
            {
                if (resp.StatusCode == HttpStatusCode.NotFound)
                    throw new FileMissingException(resp.ReasonPhrase);
                else if (!resp.IsSuccessStatusCode)
                    throw new Exception(await getResponseBodyOnErrorAsync(endpoint, resp).ConfigureAwait(false));
            }
        }


        public IList<ICommandLineArgument> SupportedCommands
        {
            get
            {
                return new List<ICommandLineArgument>(new ICommandLineArgument[] {
                    new CommandLineArgument(SIA_TARGETPATH, CommandLineArgument.ArgumentType.String, Strings.Sia.SiaPathDescriptionShort, Strings.Sia.SiaPathDescriptionLong, "/backup"),
                    new CommandLineArgument(SIA_PASSWORD, CommandLineArgument.ArgumentType.Password, Strings.Sia.SiaPasswordShort, Strings.Sia.SiaPasswordLong, null),
                    new CommandLineArgument(SIA_REDUNDANCY, CommandLineArgument.ArgumentType.String, Strings.Sia.SiaRedundancyDescriptionShort, Strings.Sia.SiaRedundancyDescriptionLong, "1.5"),
                });
            }
        }

        public string Description
        {
            get
            {
                return Strings.Sia.Description;
            }
        }

        public string[] DNSName
        {
            get { return new string[] { new System.Uri(m_apihost).Host }; }
        }

        // Sia needs to use file paths
        public bool SupportsStreaming => false;

        #endregion

        #region IDisposable Members

        public void Dispose()
        {
            m_client.Dispose();
        }

        #endregion


    }


}
