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
using System.Net.Http;
using System.Text;
using Newtonsoft.Json;
using Duplicati.Library.Utility;
using System.Net;
using System.Threading;
using System.Threading.Tasks;

namespace Duplicati.Library.Backend.Backblaze
{
    public class B2AuthHelper : JSONWebHelper
    {
        private readonly string m_credentials;
        private AuthResponse m_config;
        private DateTime m_configExpires;
        internal const string AUTH_URL = "https://api.backblazeb2.com/b2api/v1/b2_authorize_account";

        public B2AuthHelper(string userid, string password)
            : base()
        {
            m_credentials = Convert.ToBase64String(Encoding.UTF8.GetBytes(userid + ":" + password));
        }

        public override async Task<HttpRequestMessage> CreateRequestAsync(string url, string method, CancellationToken cancelToken)
        {
            var r = await base.CreateRequestAsync(url, method, cancelToken);
            r.Headers.Add("Authorization", await GetAuthorizationTokenAsync(cancelToken));
            return r;
        }

        public async Task<string> GetAuthorizationTokenAsync(CancellationToken cancelToken) 
            => (await GetConfigAsync(cancelToken)).AuthorizationToken;
        public async Task<string> GetAPIUrlAsync(CancellationToken cancelToken) 
            => (await GetConfigAsync(cancelToken)).APIUrl;
        public async Task<string> GetDownloadUrlAsync(CancellationToken cancelToken) 
            => (await GetConfigAsync(cancelToken)).DownloadUrl;
        public async Task<string> GetAccountIDAsync(CancellationToken cancelToken) 
            => (await GetConfigAsync(cancelToken)).AccountID;

        private string DropTrailingSlashes(string url)
        {
            while(url.EndsWith("/", StringComparison.Ordinal))
                url = url.Substring(0, url.Length - 1);
            return url;
        }

        public string APIDnsName
        {
            get
            {
                if (m_config == null || string.IsNullOrWhiteSpace(m_config.APIUrl))
                    return null;
                return new System.Uri(m_config.APIUrl).Host;
            }
        }

        public string DownloadDnsName
        {
            get
            {
                if (m_config == null || string.IsNullOrWhiteSpace(m_config.DownloadUrl))
                    return null;
                return new System.Uri(m_config.DownloadUrl).Host;
            }
        }

        private async Task<AuthResponse> GetConfigAsync(CancellationToken cancelToken)
        {
            if (m_config == null || m_configExpires < DateTime.UtcNow)
            {
                var retries = 0;

                while(true)
                {
                    try
                    {
                        var req = await base.CreateRequestAsync(AUTH_URL, null, cancelToken);
                        req.Headers.Add("Authorization", string.Format("Basic {0}", m_credentials));
                        //req.ContentType = "application/json; charset=utf-8";
                        
                        using(var resp = await m_client.SendAsync(req, cancelToken))
                            m_config = await ReadJSONResponseAsync<AuthResponse>(resp, cancelToken);

                        m_config.APIUrl = DropTrailingSlashes(m_config.APIUrl);
                        m_config.DownloadUrl = DropTrailingSlashes(m_config.DownloadUrl);

                        m_configExpires = DateTime.UtcNow + TimeSpan.FromHours(1);
                        return m_config;
                    }
                    catch (Exception ex)
                    {
                        var clienterror = false;

                        try
                        {
                            // Only retry once on client errors
                            if (ex is HttpRequestStatusException exception)
                            {
                                var sc = (int)exception.Response.StatusCode;
                                clienterror = (sc >= 400 && sc <= 499);
                            }
                        }
                        catch
                        {
                        }

                        if (retries >= (clienterror ? 1 : 5))
                        {
                            await AttemptParseAndThrowExceptionAsync(ex);
                            throw;
                        }

                        System.Threading.Thread.Sleep(TimeSpan.FromSeconds(Math.Pow(2, retries)));
                        retries++;
                    }
                }
            }

            return m_config;
        }

        public static async Task AttemptParseAndThrowExceptionAsync(Exception ex)
        {
            Exception newex = null;
            try
            {
                if (ex is HttpRequestStatusException exception)
                {
                    var rawdata = await exception.Response.Content.ReadAsStringAsync();
                    newex = new Exception("Raw message: " + rawdata);

                    var msg = JsonConvert.DeserializeObject<ErrorResponse>(rawdata);
                    newex = new Exception(string.Format("{0} - {1}: {2}", msg.Status, msg.Code, msg.Message));
                }
            }
            catch
            {
            }

            if (newex != null)
                throw newex;
        }

        protected override Task ParseExceptionAsync(Exception ex)
            => AttemptParseAndThrowExceptionAsync(ex);

        public static HttpStatusCode GetExceptionStatusCode(Exception ex)
        {
            if (ex is HttpRequestStatusException exception)
                return exception.Response.StatusCode;
            else
                return default(HttpStatusCode);
        }

        public Task<HttpResponseMessage> SendAsync(HttpRequestMessage requestMessage, CancellationToken cancelToken)
            => m_client.SendAsync(requestMessage, cancelToken);
            

        private class ErrorResponse
        {
            [JsonProperty("code")]
            public string Code { get; set; }
            [JsonProperty("message")]
            public string Message { get; set; }
            [JsonProperty("status")]
            public long Status { get; set; }

        }

        private class AuthResponse 
        {
            [JsonProperty("accountId")]
            public string AccountID { get; set; }
            [JsonProperty("apiUrl")]
            public string APIUrl { get; set; }
            [JsonProperty("authorizationToken")]
            public string AuthorizationToken { get; set; }
            [JsonProperty("downloadUrl")]
            public string DownloadUrl { get; set; }
        }

    }

}

