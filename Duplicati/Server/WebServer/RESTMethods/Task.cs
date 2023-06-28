#region Disclaimer / License
// Copyright (C) 2019, The Duplicati Team
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
using System;
using System.Linq;
using System.Collections.Generic;

namespace Duplicati.Server.WebServer.RESTMethods
{
    public class Task : IRESTMethodGET, IRESTMethodPOST
    {
        public void GET(string key, RequestInfo info)
        {
            var parts = (key ?? "").Split(new char[] { '/' }, 2);
            long taskid;
            if (long.TryParse(parts.FirstOrDefault(), out taskid))
            {
                var task = Program.WorkThread.CurrentTask;
                var tasks = Program.WorkThread.CurrentTasks;

                if (task != null && task.TaskID == taskid)
                {
                    info.OutputOK(new { Status = "Running" });
                    return;
                }

                if (tasks.FirstOrDefault(x => x.TaskID == taskid) == null)
                {
                    Tuple<long, Library.Interface.IBasicResults, Exception>[] matches;
                    lock(Program.MainLock)
                        matches = Program.TaskResultCache.Where(x => x.Item1 == taskid).ToArray();

                    if (matches.Length == 0)
                        info.ReportClientError("No such task found", System.Net.HttpStatusCode.NotFound);
                    else
                    {
                        Exception ex = matches[0].Item3;
                        Library.Interface.IBasicResults results = matches[0].Item2;
                        string status = "Failed";
                        string errorMessage = ex != null ? ex.Message : null;
                        if (ex == null && results != null)
                        {
                            status = results.ParsedResult == Library.Interface.ParsedResultType.Success ? "Success": results.ParsedResult.ToString();
                            if (results.Errors.Any())
                            {
                                errorMessage = string.Join(Environment.NewLine, results.Errors);
                            }
                        }
                        info.OutputOK(new
                        {
                            Status = status,
                            ErrorMessage = errorMessage,
                            Exception = ex == null ? null : ex.ToString()
                        });
                    }
                }
                else
                {
                    info.OutputOK(new { Status = "Waiting" });
                }
            }
            else
            {
                info.ReportClientError("Invalid request", System.Net.HttpStatusCode.BadRequest);
            }
        }

        public void POST(string key, RequestInfo info)
        {
            var parts = (key ?? "").Split(new char[] { '/' }, 2);
            long taskid;
            if (parts.Length == 2 && long.TryParse(parts.First(), out taskid))
            {
                var task = Program.WorkThread.CurrentTask;
                var tasks = Program.WorkThread.CurrentTasks;

                if (task != null)
                    tasks.Insert(0, task);

                task = tasks.FirstOrDefault(x => x.TaskID == taskid);
                if (task == null)
                {
                    info.ReportClientError("No such task", System.Net.HttpStatusCode.NotFound);
                    return;
                }

                switch (parts.Last().ToLowerInvariant())
                {
                    case "stopaftercurrentfile":
                        task.Stop(allowCurrentFileToFinish: true);
                        info.OutputOK();
                        return;

                    case "stopnow":
                        task.Stop(allowCurrentFileToFinish: false);
                        info.OutputOK();
                        return;

                    case "abort":
                        task.Abort();
                        info.OutputOK();
                        return;
                }
            }

            info.ReportClientError("Invalid or missing task id", System.Net.HttpStatusCode.NotFound);
        }
    }
}

