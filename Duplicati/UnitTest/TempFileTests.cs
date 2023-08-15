using Duplicati.Library.Utility;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Duplicati.UnitTest
{
    [TestFixture]
    public class TempFileTests
    {

        [Test]
        [Category("TempFile")]
        [TestCase(false)]
        [TestCase(true)]
        public void CreateAndRead(bool memory)
        {
            ITempFile tmp;
            if (memory)
            {
                tmp = new TempMemoryFile();
            }
            else
            {
                tmp = new TempFile();
            }
            using (tmp)
            {
                byte[] testData = new byte[10 * 1024];
                WriteData(tmp, testData);
                // Try to read multiple times
                for (int i = 0; i < 3; ++i)
                {
                    Assert.AreEqual(testData.Length, tmp.Length);
                    byte[] res = ReadData(tmp);
                    Assert.AreEqual(testData, res);
                }
            }
            if (!memory)
            {
                // Check that temp file is deleted with dispose
                Assert.IsFalse(File.Exists((tmp as TempFile)));
            }
        }

        [Test]
        [Category("TempFile")]
        [TestCase(-1)]
        [TestCase(10 * 1024)]
        [TestCase(1024 * 1024 * 1024)]
        public void CreateEither(long sizeHint)
        {
            ITempFile f = TempFile.Create(sizeHint);
            Assert.IsNotNull(f);
            Assert.IsTrue(f is TempFile || f is TempMemoryFile);
        }

        [Test]
        [Category("TempFile")]
        [TestCase(false)]
        [TestCase(true)]
        public void ToDiskFile(bool memory)
        {
            ITempFile tmp;
            if (memory)
            {
                tmp = new TempMemoryFile();
            }
            else
            {
                tmp = new TempFile();
            }
            string filename;
            using (TempFile.ToDiskFile(tmp, out filename))
            {
                Assert.IsTrue(File.Exists(filename));
            }
            if (!memory)
            {
                Assert.AreEqual((tmp as TempFile).Name, filename);
            }
            Assert.AreEqual(!memory, File.Exists(filename));

            byte[] testData = new byte[10 * 1024];
            WriteData(tmp, testData);
            using (TempFile.ToDiskFile(tmp, out filename))
            {
                MemoryStream read = new MemoryStream();
                using (read)
                using (var s = File.OpenRead(filename))
                {
                    Utility.CopyStream(s, read);
                }
                byte[] res = read.ToArray();
                Assert.AreEqual(testData, res);
            }
        }

        private static void WriteData(ITempFile tmp, byte[] testData)
        {
            using (var s = tmp.OpenWrite())
            {
                Utility.CopyStream(new MemoryStream(testData), s);
            }
        }

        private static byte[] ReadData(ITempFile tmp)
        {
            MemoryStream read = new MemoryStream();
            using (read)
            using (var s = tmp.OpenRead())
            {
                Utility.CopyStream(s, read);
            }
            byte[] res = read.ToArray();
            return res;
        }
    }
}
