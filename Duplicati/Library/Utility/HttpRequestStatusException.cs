using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Text;

namespace Duplicati.Library.Utility
{
    /// <summary>
    /// Exception that contains a HttpResposeMessage. The response might be disposed already, so it should be used carefully (accessing the content might fail)
    /// </summary>
    public class HttpRequestStatusException : HttpRequestException
    {
        public readonly HttpResponseMessage Response;

        public HttpRequestStatusException(HttpResponseMessage resp)
            : base(resp.ReasonPhrase)
        {
            Response = resp;
        }

        public HttpRequestStatusException(string message, HttpResponseMessage resp)
            : base(message)
        {
            Response = resp;
        }
    }
}
