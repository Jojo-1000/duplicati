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
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Duplicati.Library.Interface
{
    /// <summary>
    /// Contains extension methods for the IBackend interfaces
    /// </summary>
    public static class BackendExtensions
    {
        /// <summary>
        /// Tests a backend by invoking the ListAsync() method.
        /// As long as the iteration can either complete or find at least one file without throwing, the test is successful
        /// </summary>
        /// <param name="backend">Backend to test</param>
        /// <param name="token">The cancellation token to use</param>
        /// <returns>An awaitable task</returns>
        public static async Task TestListAsync(this IBackend backend, CancellationToken token)
        {
            if (backend is IBackendPagination backendPagination)
            {
                await foreach(var res in backendPagination.ListEnumerableAsync(token))
                    break;
            }
            else
            {
            // If we can iterate successfully, even if it's empty, then the backend test is successful
                foreach(var res in await backend.ListAsync(token))
                    break;
            }
        }

        /// <summary>
        /// Converts a paginated list into a condensed simple list
        /// </summary>
        /// <param name="backend">The pagination enabled backend</param>
        /// <param name="token">The cancellation token to use</param>
        /// <returns>The complete list</returns>
        public static async Task<IList<IFileEntry>> CondensePaginatedListAsync(this IBackendPagination backend, CancellationToken token)
        {
            var lst = new List<IFileEntry>();
            await foreach(var n in backend.ListEnumerableAsync(token))
                lst.Add(n);

            return lst;
        }
    }
}
