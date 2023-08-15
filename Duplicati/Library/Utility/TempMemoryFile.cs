using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Duplicati.Library.Utility
{
    public class TempMemoryFile : ITempFile
    {
        private MemoryStream m_stream;
        // Data once stream is closed
        private byte[] m_data = null;

        public long Length
        {
            get
            {
                if (m_stream.CanRead)
                {
                    return m_stream.Length;
                }
                else
                {
                    if (m_data == null)
                    {
                        // TODO: Find out a way to not close m_stream, so Length can be used and the array does not need to be copied
                        m_data = m_stream.ToArray();
                    }
                    return m_data.Length;
                }
            }
        }

        public TempMemoryFile(int capacity)
        {
            m_stream = new MemoryStream(capacity);
        }

        public TempMemoryFile()
        {
            m_stream = new MemoryStream();
        }

        public void Dispose()
        {
            m_stream?.Dispose();
        }

        public Stream OpenRead()
        {
            if (m_data == null)
            {
                m_data = m_stream.ToArray();
            }
            return new MemoryStream(m_data);
        }

        public Stream OpenWrite()
        {
            m_data = null;
            if (m_stream.CanWrite)
            {
                // Overwrite data
                m_stream.SetLength(0);
                return m_stream;
            }
            else
            {
                // Stream was already closed, open new one
                m_stream = new MemoryStream(m_stream.Capacity);
                return m_stream;
            }
        }
        public Stream OpenReadWrite()
        {
            return OpenWrite();
        }
    }
}
