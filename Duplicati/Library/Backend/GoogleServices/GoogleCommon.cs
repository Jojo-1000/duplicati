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
using Duplicati.Library.Utility;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading;
using System.Threading.Tasks;

namespace Duplicati.Library.Backend.GoogleServices
{
    internal static class GoogleCommon
    {
        /// <summary>
        /// The size of upload chunks
        /// </summary>
        private const long UPLOAD_CHUNK_SIZE = 1024 * 1024 * 10;

        /// <summary>
        /// Helper method that queries a resumeable upload uri for progress
        /// </summary>
        /// <returns>A pair of upload range and response (or null).</returns>
        /// <param name="oauth">The Oauth instance</param>
        /// <param name="uploaduri">The resumeable uploaduri</param>
        /// <param name="streamlength">The length of the entire stream</param>
        private static async Task<KeyValuePair<long, T>> QueryUploadRangeAsync<T>(OAuthHelper oauth, string uploaduri, long streamlength, CancellationToken cancelToken)
            where T : class
        {
            T response = null;
            var req = await oauth.CreateRequestAsync(uploaduri, "PUT", cancelToken);
            req.Content.Headers.ContentLength = 0;
            req.Content.Headers.ContentRange = new ContentRangeHeaderValue(streamlength);

            using(var resp = await oauth.GetResponseWithoutExceptionAsync(req, null, cancelToken))
            {
                var code = (int)resp.StatusCode;

                // If the upload is completed, we get 201 or 200
                if (code >= 200 && code <= 299)
                {
                    response = await oauth.ReadJSONResponseAsync<T>(resp, cancelToken);
                    if (response == null)
                        throw new Exception(string.Format("Upload succeeded, but no data was returned, status code: {0}", code));
                    
                    return new KeyValuePair<long, T>(streamlength, response);
                }

                if (code == 308)
                {
                    // A lack of a Range header is undocumented, 
                    // but seems to occur when no data has reached the server:
                    // https://code.google.com/a/google.com/p/apps-api-issues/issues/detail?id=3884

                    if (!resp.Headers.TryGetValues("Range", out IEnumerable<string> range))
                        return new KeyValuePair<long, T>(0, response);
                    else
                        return new KeyValuePair<long, T>(long.Parse(range.First().Split(new char[] { '-' })[1]) + 1, response);
                }
                else
                    throw new HttpRequestStatusException(string.Format("Unexpected status code: {0}", code), resp);
            }
        }

        /// <summary>
        /// Uploads the requestdata as JSON and starts a resumeable upload session, then uploads the stream in chunks
        /// </summary>
        /// <returns>Serialized result of the last request.</returns>
        /// <param name="oauth">The Oauth instance.</param>
        /// <param name="requestdata">The data to submit as JSON metadata.</param>
        /// <param name="url">The URL to register the upload session against.</param>
        /// <param name="stream">The stream with content data to upload.</param>
        /// <param name="method">The HTTP Method.</param>
        /// <typeparam name="TRequest">The type of data to upload as metadata.</typeparam>
        /// <typeparam name="TResponse">The type of data returned from the upload.</typeparam>
        public static async Task<TResponse> ChunkedUploadWithResumeAsync<TRequest, TResponse>(OAuthHelper oauth, TRequest requestdata, string url, System.IO.Stream stream, CancellationToken cancelToken, string method = "POST")
            where TRequest : class
            where TResponse : class
        {
            var req = await oauth.CreateRequestAsync(url, method, cancelToken);
            
            req.Headers.Add("X-Upload-Content-Type", "application/octet-stream");
            req.Headers.Add("X-Upload-Content-Length", stream.Length.ToString());

            string uploaduri;
            using(var resp = await oauth.GetResponseAsync(req, requestdata, cancelToken))
            {
                if (resp.StatusCode != HttpStatusCode.OK || resp.Headers.Location == null)
                    throw new HttpRequestStatusException("Failed to start upload session", resp);

                uploaduri = resp.Headers.Location.AbsoluteUri;
            }

            return await ChunkedUploadAsync<TResponse>(oauth, uploaduri, stream, cancelToken).ConfigureAwait(false);
        }

        /// <summary>
        /// Helper method that performs a chunked upload, and queries for http status after each chunk
        /// </summary>
        /// <returns>The response item</returns>
        /// <param name="oauth">The Oauth instance</param>
        /// <param name="uploaduri">The resumeable uploaduri</param>
        /// <param name="stream">The stream with data to upload.</param>
        /// <typeparam name="T">The type of data in the response.</typeparam>
        private static async Task<T> ChunkedUploadAsync<T>(OAuthHelper oauth, string uploaduri, System.IO.Stream stream, CancellationToken cancelToken)
            where T : class
        {
            var queryRange = false;
            var retries = 0;
            var offset = 0L;

            // Repeatedly try uploading until all retries are done
            while(true)
            {
                try
                {
                    if (queryRange)
                    {

                        KeyValuePair<long,T> re = await QueryUploadRangeAsync<T>(oauth, uploaduri, stream.Length, cancelToken);
                        offset = re.Key;
                        queryRange = false;

                        if (re.Value != null)
                            return re.Value;
                    }

                    //Seek into the right place
                    if (stream.Position != offset)
                        stream.Position = offset;

                    var req = await oauth.CreateRequestAsync(uploaduri, "PUT", cancelToken);

                    var chunkSize = Math.Min(UPLOAD_CHUNK_SIZE, stream.Length - offset);

                    // Upload data in chunks
                    using (var body = new StreamContent(new PartialStream(stream, offset, chunkSize)))
                    {
                        body.Headers.ContentType = MediaTypeHeaderValue.Parse("application/octet-stream");
                        body.Headers.ContentLength = chunkSize;

                        req.Content = body;
                        req.Content.Headers.ContentRange = new ContentRangeHeaderValue(offset, offset + chunkSize - 1, stream.Length);

                        // Check the response
                        using (var resp = await oauth.GetResponseWithoutExceptionAsync(req, null, cancelToken))
                        {
                            var code = (int)resp.StatusCode;

                            if (code == 308 && resp.Headers.TryGetValues("Range", out IEnumerable<string> range))
                            {
                                offset = long.Parse(range.First().Split(new char[] { '-' })[1]) + 1;
                                retries = 0;
                            }
                            else if (code >= 200 && code <= 299)
                            {
                                offset += chunkSize;
                                if (offset != stream.Length)
                                    throw new Exception(string.Format("Upload succeeded prematurely. Uploaded: {0}, total size: {1}", offset, stream.Length));

                                //Verify that the response is also valid
                                var res = await oauth.ReadJSONResponseAsync<T>(resp, cancelToken);
                                if (res == null)
                                    throw new Exception(string.Format("Upload succeeded, but no data was returned, status code: {0}", code));

                                return res;
                            }
                            else
                            {
                                throw new HttpRequestStatusException(string.Format("Unexpected status code: {0}", code), resp);
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    var retry = false;

                    // If we get a 5xx error, or some network issue, we retry
                    if (ex is HttpRequestStatusException exception)
                    {
                        var code = (int)exception.Response.StatusCode;
                        retry = code >= 500 && code <= 599;
                    }
                    else if (ex is System.Net.Sockets.SocketException || ex is System.IO.IOException || ex.InnerException is System.Net.Sockets.SocketException || ex.InnerException is System.IO.IOException)
                    {
                        retry = true;
                    }

                    // Retry with exponential backoff
                    if (retry && retries < 5)
                    {
                        System.Threading.Thread.Sleep(TimeSpan.FromSeconds(Math.Pow(2, retries)));
                        retries++;

                        // Ask server where we left off
                        queryRange = true;
                    }
                    else
                        throw;
                }
            }
        }
    }
}

