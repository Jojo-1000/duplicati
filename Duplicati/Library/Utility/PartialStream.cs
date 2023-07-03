using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Duplicati.Library.Utility
{
    /// <summary>
    /// Stream that reads a certain length from an input source in parts. Does not dispose the source stream
    /// </summary>
    public class PartialStream : Stream
    {
        private readonly Stream m_source;
        private readonly long m_offset;
        private readonly long m_length;
        private long m_position;

        public PartialStream(Stream source, long offset, long length)
        {
            m_source = source ?? throw new ArgumentNullException(nameof(source));
            m_offset = offset;
            m_length = length;
        }

        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => m_length;
        public override long Position { get => m_position; set => throw new NotSupportedException(); }

        public override void Flush()
        {
        }

        public override async Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            var c = (int)Math.Min(count, m_length - m_position);
            var r = await m_source.ReadAsync(buffer, offset, c, cancellationToken);
            m_position += r;
            return r;
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            var c = (int)Math.Min(count, m_length - m_position);
            var r = m_source.Read(buffer, offset, c);
            m_position += r;
            return r;
        }


        public override long Seek(long offset, SeekOrigin origin)
        {
            throw new NotSupportedException();
        }

        public override void SetLength(long value)
        {
            throw new NotSupportedException();
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            throw new NotSupportedException();
        }
    }
}
