using System;
using System.IO;

namespace Duplicati.Library.Common.IO
{
    /// <summary>
    /// Wrapper of a filename which optionally opens as a FileStream
    /// </summary>
    /// This class is used to support backends which require a file path and cannot handle streams, or if streaming transfers are disabled.
    /// They can use Filename to get the path. Other backends can use this stream as normal if necessary.
    public class FauxStream : Stream
    {
        public FauxStream(string path, FileMode mode, FileAccess access, FileShare share)
        {
            Filename = path;
            m_mode = mode;
            m_access = access;
            m_share = share;
        }

        // Fallback stream for backends that use the stream api
        private FileStream m_stream;
        private readonly FileMode m_mode;
        private readonly FileAccess m_access;
        private readonly FileShare m_share;

        private void OpenStream()
        {
            if (m_stream == null)
            {
                m_stream = new FileStream(Filename, m_mode, m_access, m_share);
            }
        }

        public string Filename { get; }

        public override bool CanRead
        {
            get
            {
                OpenStream();
                return m_stream.CanRead;
            }
        }

        public override bool CanSeek
        {
            get
            {
                OpenStream();
                return m_stream.CanSeek;
            }
        }

        public override bool CanWrite
        {
            get
            {
                OpenStream();
                return m_stream.CanWrite;
            }
        }

        public override long Length
        {
            get
            {
                OpenStream();
                return m_stream.Length;
            }
        }

        public override long Position
        {
            get
            {
                OpenStream();
                return m_stream.Position;
            }
            set
            {
                OpenStream();
                m_stream.Position = value;
            }
        }

        public override void Flush()
        {
            OpenStream();
            m_stream.Flush();
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            OpenStream();
            return m_stream.Read(buffer, offset, count);
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            OpenStream();
            return m_stream.Seek(offset, origin);
        }

        public override void SetLength(long value)
        {
            OpenStream();
            m_stream.SetLength(value);
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            OpenStream();
            m_stream.Write(buffer, offset, count);
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                m_stream?.Dispose();
                m_stream = null;
            }
            base.Dispose(disposing);
        }

        /// <summary>
        /// Opens a FileStream if supported, otherwise returns a FauxStream
        /// </summary>
        /// <param name="supportsStreaming">Whether the backend supports streaming</param>
        /// <param name="path">Path to the file</param>
        /// <param name="access">File access mode</param>
        /// <returns>Instance of FileStream or FauxStream for the path</returns>
        public static Stream OpenSupported(bool supportsStreaming, string path, FileAccess access)
        {
            // Emulate mode and share of File.OpenRead/OpenWrite
            FileMode mode = (access == FileAccess.Read) ? FileMode.Open : FileMode.OpenOrCreate;
            FileShare share = (access == FileAccess.Read) ? FileShare.Read : FileShare.None;
            if(supportsStreaming)
            {
                return new FileStream(path, mode, access, share);
            }
            else
            {
                return new FauxStream(path, mode, access, share);
            }
        }
    }
}
