using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Text;

namespace Duplicati.Library.Utility
{
    public static class RequestUtility
    {
        /// <summary>
        /// Add cookie header to the request.
        /// </summary>
        /// Requires that UseCookies is set to false on the HttpClientHandler, otherwise it is overwritten.
        /// <param name="request">Request to add cookies to</param>
        /// <param name="cookies">Collection of cookies which should be added</param>
        public static void AddCookies(HttpRequestMessage request, CookieCollection cookies)
        {
            if (cookies.Count > 0)
            {
                CookieContainer container = new CookieContainer();
                container.Add(request.RequestUri, cookies);
                request.Headers.Add("Cookie", container.GetCookieHeader(request.RequestUri));
            }
        }

        /// <summary>
        /// Parse cookies from a http response.
        /// </summary>
        /// <param name="response">Response with Set-Cookie headers</param>
        /// <returns>The collection of cookies for the response</returns>
        public static CookieCollection ParseCookies(HttpResponseMessage response)
        {
            CookieContainer responseCookies = new CookieContainer();
            var uri = new System.Uri("http://placeholder/");
            foreach (var c in response.Headers.GetValues("Set-Cookie"))
                responseCookies.SetCookies(uri, c);

            return responseCookies.GetCookies(uri);
        }
    }
}
