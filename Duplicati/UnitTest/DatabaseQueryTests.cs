using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Duplicati.Library.Main.Database;
using System.IO;
using Duplicati.Library.Main;
using Duplicati.Library.Utility;
using Duplicati.Library.Logging;

namespace Duplicati.UnitTest
{
    [TestFixture]
    class DatabaseQueryTests : BasicSetupHelper
    {
        private readonly string testDbFilepath = Path.Combine(BASEFOLDER, "test-db.sqlite");

        private IDisposable logScope;

        private readonly string LOGTAG = "DatabaseQueryTest";

        [SetUp]
        public override void SetUp()
        {
            base.SetUp();
            if (!systemIO.FileExists(testDbFilepath))
            {
                Assert.Inconclusive("No test DB in test folder!");
            }
            File.Copy(testDbFilepath, DBFILE, true);
            Log.WriteInformationMessage(LOGTAG, "StartTestMethod", "{0} started",
                TestContext.CurrentContext.Test.MethodName);
        }

        [OneTimeSetUp]
        public override void OneTimeSetUp()
        {
            base.OneTimeSetUp();
            logScope = Log.StartScope(new StreamLogDestination(LOGFILE), LogMessageType.Profiling);
            Log.WriteInformationMessage(LOGTAG, "StartTest", "DatabaseQueryTest started");
            KeepLogfile = true;
        }

        [OneTimeTearDown]
        public override void OneTimeTearDown()
        {
            Log.WriteInformationMessage(LOGTAG, "StartTest", "DatabaseQueryTest finished");
            if (logScope != null)
            {
                logScope.Dispose();
            }
            base.OneTimeTearDown();
        }

        [Test]
        [Category("Database")]
        public void Local()
        {
            using (var db = new LocalDatabase(DBFILE, "database-test", true))
            {
                using (var tr = db.BeginTransaction())
                {
                    Options opt = new Options(new Dictionary<string, string>());
                    Library.Main.Utility.UpdateOptionsFromDb(db, opt, tr);
                    var remoteVolumes = db.GetRemoteVolumes(tr).ToList();
                    RemoteVolumeEntry filesetEntry = RemoteVolumeEntry.Empty;
                    RemoteVolumeEntry indexEntry = RemoteVolumeEntry.Empty;
                    RemoteVolumeEntry blockEntry = RemoteVolumeEntry.Empty;
                    foreach (RemoteVolumeEntry e in remoteVolumes)
                    {
                        if (e.Type == Library.Main.RemoteVolumeType.Files)
                        {
                            filesetEntry = e;
                        }
                        else if (e.Type == Library.Main.RemoteVolumeType.Index)
                        {
                            indexEntry = e;
                        }
                        else if (e.Type == Library.Main.RemoteVolumeType.Blocks)
                        {
                            blockEntry = e;
                        }
                        if (blockEntry.ID != -1 && indexEntry.ID != -1 && blockEntry.ID != -1)
                        {
                            break;
                        }
                    }
                    remoteVolumes = null;
                    int hashsize = HashAlgorithmHelper.Create(opt.BlockHashAlgorithm).HashSize / 8;

                    //db.VerifyConsistency(opt.Blocksize, hashsize, true, tr);
                    db.VerifyConsistency(opt.Blocksize, hashsize, false, tr);

                    db.UpdateRemoteVolume(filesetEntry.Name, RemoteVolumeState.Deleting, filesetEntry.Size, filesetEntry.Hash, tr);

                    var filesetTimes = db.FilesetTimes.ToList();

                    long indexId = db.GetRemoteVolumeID(indexEntry.Name, tr);
                    Assert.AreEqual(indexEntry.ID, indexId);
                    RemoteVolumeEntry indexEntry2 = db.GetRemoteVolume(indexEntry.Name, tr);
                    Assert.AreEqual(indexEntry, indexEntry2);

                    {
                        // Create duplicate
                        long duplicateIdx = db.RegisterRemoteVolume(indexEntry.Name, indexEntry.Type, indexEntry.Size, RemoteVolumeState.Deleting);
                        var duplicateVolumes = db.DuplicateRemoteVolumes().ToList();
                        Assert.IsTrue(duplicateVolumes.Any());

                        // Remove duplicate entry
                        db.UnlinkRemoteVolume(indexEntry.Name, RemoteVolumeState.Deleting, tr);
                        var duplicateVolumes2 = db.DuplicateRemoteVolumes().ToList();
                        Assert.AreEqual(duplicateVolumes.Count - 1, duplicateVolumes2.Count);
                    }

                    db.LogMessage("Type", "Message", new Exception("Exception"), tr);

                    var time = DateTime.Now;
                    long fsId = db.CreateFileset(filesetEntry.ID, time, tr);
                    {
                        var filesetIds = db.GetFilesetIDs(time, null).ToList();
                        Assert.Contains(fsId, filesetIds);
                        filesetIds = db.FindMatchingFilesets(time, null).ToList();
                        Assert.Contains(fsId, filesetIds);
                        var filesetTimes2 = db.FilesetTimes.ToList();
                        // Milliseconds are not saved in database
                        var compareTime = new DateTime(time.Year, time.Month, time.Day,
                            time.Hour, time.Minute, time.Second, time.Kind).ToLocalTime();
                        Assert.Contains(new KeyValuePair<long, DateTime>(fsId, compareTime), filesetTimes2);
                    }
                    {
                        long fullBlockCount = db.GetBlocksLargerThan(opt.Blocksize - 1);

                        var blocks = db.GetBlocks(blockEntry.ID, tr).Take(100).ToList();
                    }

                    db.AddIndexBlockLink(indexEntry.ID, blockEntry.ID, tr);

                    var blocklists = db.GetBlocklists(blockEntry.ID, opt.Blocksize, hashsize, tr).Take(10).ToList();

                    db.UpdateFullBackupStateInFileset(fsId, true, tr);
                    Assert.IsTrue(db.IsFilesetFullBackup(time));

                    var splitPath = LocalDatabase.SplitIntoPrefixAndName("/test/prefix/file.txt");
                    long prefixId = db.GetOrCreatePathPrefix(splitPath.Key, tr);

                    db.RemoveRemoteVolume(blockEntry.Name, tr);

                    db.RepairInProgress = true;
                    Assert.IsTrue(db.RepairInProgress);
                    db.RepairInProgress = false;
                    Assert.IsFalse(db.RepairInProgress);

                    db.PartiallyRecreated = true;
                    Assert.IsTrue(db.PartiallyRecreated);
                    db.PartiallyRecreated = false;
                    Assert.IsFalse(db.PartiallyRecreated);

                    db.RenameRemoteFile(filesetEntry.Name, filesetEntry.Name + "_", tr);

                    // For purge deleted volumes
                    db.UpdateRemoteVolume(indexEntry.Name, RemoteVolumeState.Deleted, indexEntry.Size, indexEntry.Hash, false, TimeSpan.FromTicks(1), tr);

                    tr.Commit();
                }
                db.Vacuum();
                db.PurgeLogData(DateTime.UtcNow);
                db.PurgeDeletedVolumes(DateTime.UtcNow);
            }
        }

        [Test]
        [Category("Database")]
        public void Backup()
        {
            using (var parentdb = new LocalDatabase(DBFILE, "database-test", true))
            {
                Options opt = new Options(new Dictionary<string, string>());
                Library.Main.Utility.UpdateOptionsFromDb(parentdb, opt);
                using (var db = new LocalBackupDatabase(parentdb, opt))
                {
                    using (var tr = db.BeginTransaction())
                    {
                        var filesetEntries = (from v in db.GetRemoteVolumes(tr)
                                              where v.Type == RemoteVolumeType.Files
                                              select v).Take(5).ToList();

                        var blockEntries = (from v in db.GetRemoteVolumes(tr)
                                            where v.Type == RemoteVolumeType.Blocks
                                            select v).Take(5).ToList();
                        int hashsize = HashAlgorithmHelper.Create(opt.BlockHashAlgorithm).HashSize / 8;

                        Assert.IsTrue(db.AddBlock("test-hash", 10, blockEntries[0].ID, tr));
                        Assert.AreNotEqual(-1, db.FindBlockID("test-hash", 10, tr));

                        string filehash = "test-hash";
                        long filesize = 10;
                        long filesetId = db.CreateFileset(filesetEntries[0].ID, DateTime.Now, tr);
                        Assert.IsTrue(db.AddBlockset(filehash, filesize, opt.Blocksize, new[] { "test-hash" }, null, out long blocksetid, tr));
                        Assert.IsTrue(db.AddMetadataset(filehash, filesize, blocksetid, out long metadataid, tr));
                        Assert.IsTrue(db.GetMetadatasetID(filehash, filesize, out long foundMetadataid, tr));
                        Assert.AreEqual(metadataid, foundMetadataid);
                        db.AddFile("test/path/1", DateTime.Now, blocksetid, metadataid, tr);
                        db.AddDirectoryEntry("test/path/2", metadataid, DateTime.Now, tr);
                        db.AddSymlinkEntry("test/path/3", metadataid, DateTime.Now, tr);
                        var split = LocalDatabase.SplitIntoPrefixAndName(db.GetFirstPath());
                        long prefixId = db.GetOrCreatePathPrefix(split.Key, tr);
                        db.GetFileLastModified(prefixId, split.Value, filesetId, true, out DateTime modified, out long length, tr);
                        long fileid = db.GetFileEntry(prefixId, split.Value, filesetId, out modified, out filesize, out string oldMetahash, out long oldMetasize, tr);
                        db.AddUnmodifiedFile(fileid, modified, tr);
                        db.GetMetadataHashAndSizeForFile(fileid, tr);
                        db.GetFileHash(fileid, tr);
                        db.GetIncompleteFilesets(tr).ToList();
                        db.GetRemoteVolumeFromFilesetID(filesetId, tr);
                        db.GetTemporaryFilelistVolumeNames(false, tr).ToList();
                        db.GetMissingIndexFiles(tr).ToList();
                        db.LinkFilesetToVolume(filesetId, filesetEntries[0].ID, tr);

                        db.GetLastBackupFileCountAndSize();
                        db.AppendFilesFromPreviousSet(tr);

                        //db.MoveBlockToVolume(filehash, filesize, filesetEntries[0].ID, 
                        Assert.Catch<Exception>(() => { db.SafeDeleteRemoteVolume(blockEntries[2].Name, tr); });
                        db.GetBlocklistHashes(filehash, tr);
                        var journal = db.GetChangeJournalData(filesetId).ToList();
                        db.UpdateChangeJournalData(journal, filesetId, tr);
                        db.UpdateChangeStatistics(new BackupResults(), tr);

                        tr.Rollback();
                    }
                }
            }
        }

        [Test]
        [Category("Database")]
        public void Delete()
        {
            using (var db = new LocalDeleteDatabase(DBFILE, "database-test"))
            {
                using (var tr = db.BeginTransaction())
                {
                    Options opt = new Options(new Dictionary<string, string>());
                    Library.Main.Utility.UpdateOptionsFromDb(db, opt, tr);

                    var filesets = db.FilesetsWithBackupVersion.ToList();

                    var filesetEntry = (from v in db.GetRemoteVolumes(tr)
                                        where v.Type == RemoteVolumeType.Files
                                        select v).First();
                    var blockEntries = (from v in db.GetRemoteVolumes(tr)
                                        where v.Type == RemoteVolumeType.Blocks
                                        select v).Take(2).ToList();
                    db.DropFilesetsFromTable(new[] { filesets.First().Time }, tr);

                    var compactReport = db.GetCompactReport(0, 0, 0, 0, tr);
                    compactReport.ReportCompactData();

                    var block = db.GetBlocks(blockEntries[0].ID, tr).First();
                    db.MoveBlockToNewVolume(block.Hash, block.Size, blockEntries[1].ID, tr);

                    var blockVolume = new RemoteVolume(blockEntries[0].Name, blockEntries[0].Hash, blockEntries[0].Size);
                    var deletable = db.GetDeletableVolumes(new[] { blockVolume }, tr);
                    Assert.IsNotEmpty(deletable);

                    tr.Rollback();
                }
            }
        }

        [Test]
        [Category("Database")]
        public void BugReport()
        {
            using (var db = new LocalBugReportDatabase(DBFILE))
            {
                db.Fix();
            }
        }

        [Test]
        [Category("Database")]
        public void ListAffected()
        {
            using (var db = new LocalListAffectedDatabase(DBFILE))
            {
                using (var tr = db.BeginTransaction())
                {
                    Options opt = new Options(new Dictionary<string, string>());
                    Library.Main.Utility.UpdateOptionsFromDb(db, opt, tr);

                    var filesetEntries = (from v in db.GetRemoteVolumes(tr)
                                          where v.Type == RemoteVolumeType.Files
                                          select v).Take(5).ToList();

                    var blockEntries = (from v in db.GetRemoteVolumes(tr)
                                        where v.Type == RemoteVolumeType.Blocks
                                        select v).Take(5).ToList();

                    var affectedFilesets = db.GetFilesets(blockEntries.Select(x => x.Name)).ToList();
                    Assert.IsNotEmpty(affectedFilesets);

                    var affectedFiles = db.GetFiles(blockEntries.Select(x => x.Name)).ToList();
                    Assert.IsNotEmpty(affectedFiles);

                    db.LogMessage("Test", "Test message", null, tr);
                    var logLines = db.GetLogLines(new[] { "Test message" });
                    // TODO: Function appends ' ' to the end of messages, even if there is no exception
                    Assert.IsNotEmpty(logLines.Where(x => x.Message.Contains("Test message")));

                    var affectedVolumes = db.GetVolumes(filesetEntries.Select(x => x.Name)).ToList();
                    Assert.IsNotEmpty(affectedVolumes);

                    tr.Rollback();
                }
            }
        }

        [Test]
        [Category("Database")]
        public void ListBrokenFiles()
        {
            using (var db = new LocalListBrokenFilesDatabase(DBFILE))
            {
                using (var tr = db.BeginTransaction())
                {
                    Options opt = new Options(new Dictionary<string, string>());
                    Library.Main.Utility.UpdateOptionsFromDb(db, opt, tr);

                    var filesetEntries = (from v in db.GetRemoteVolumes(tr)
                                          where v.Type == RemoteVolumeType.Files
                                          select v).Take(5).ToList();

                    var blockEntries = (from v in db.GetRemoteVolumes(tr)
                                        where v.Type == RemoteVolumeType.Blocks
                                        select v).Take(5).ToList();


                    var brokenFilesets = db.GetBrokenFilesets(new DateTime(0), null, tr).ToList();

                    var brokenFilenames = db.GetBrokenFilenames(filesetEntries[0].ID, tr).ToList();

                    // TODO: db.InsertBrokenFileIDsIntoTable()

                    db.RemoveMissingBlocks(blockEntries.Select(x => x.Name), tr);

                    db.GetFilesetFileCount(filesetEntries[0].ID, tr);

                    tr.Rollback();
                }
            }
        }

        [Test]
        [Category("Database")]
        public void ListChanges()
        {
            using (var db = new LocalListChangesDatabase(DBFILE))
            {
                List<RemoteVolumeEntry> filesetEntries;
                List<RemoteVolumeEntry> blockEntries;
                using (var tr = db.BeginTransaction())
                {
                    Options opt = new Options(new Dictionary<string, string>());
                    Library.Main.Utility.UpdateOptionsFromDb(db, opt, tr);

                    filesetEntries = (from v in db.GetRemoteVolumes(tr)
                                      where v.Type == RemoteVolumeType.Files
                                      select v).Take(5).ToList();

                    blockEntries = (from v in db.GetRemoteVolumes(tr)
                                    where v.Type == RemoteVolumeType.Blocks
                                    select v).Take(5).ToList();
                    tr.Rollback();
                }
                using (var helper = db.CreateStorageHelper())
                {
                    helper.AddFromDb(filesetEntries[0].ID, true, new FilterExpression("*"));
                    helper.AddElement("testpath", "testhash", "testmetahash", 100, Library.Interface.ListChangesElementType.File, true);

                    helper.CreateChangeSizeReport();
                    helper.CreateChangeCountReport();
                    helper.CreateChangedFileReport().ToList();
                }
            }
        }
        [Test]
        [Category("Database")]
        public void List()
        {
            using (var db = new LocalListDatabase(DBFILE))
            {
                using (var tr = db.BeginTransaction())
                {
                    Options opt = new Options(new Dictionary<string, string>());
                    Library.Main.Utility.UpdateOptionsFromDb(db, opt, tr);

                    var filesetEntries = (from v in db.GetRemoteVolumes(tr)
                                          where v.Type == RemoteVolumeType.Files
                                          select v).Take(5).ToList();

                    var blockEntries = (from v in db.GetRemoteVolumes(tr)
                                        where v.Type == RemoteVolumeType.Blocks
                                        select v).Take(5).ToList();

                    using (var fileSets = db.SelectFileSets(new DateTime(0), new[] { 0L }))
                    {
                        var filter = new FilterExpression("C:\\Users\\");
                        fileSets.GetLargestPrefix(filter).Take(100).ToList();
                        fileSets.SelectFolderContents(filter).Take(100).ToList();
                        fileSets.SelectFiles(filter).Take(100).ToList();
                        fileSets.QuickSets.Take(10).ToList();
                        fileSets.Sets.Take(10).ToList();
                    }

                    tr.Rollback();
                }
            }
        }

        [Test]
        [Category("Database")]
        public void Purge()
        {
            using (var db = new LocalPurgeDatabase(DBFILE))
            {
                using (var tr = db.BeginTransaction())
                {
                    Options opt = new Options(new Dictionary<string, string>());
                    Library.Main.Utility.UpdateOptionsFromDb(db, opt, tr);

                    var filesetEntries = (from v in db.GetRemoteVolumes(tr)
                                          where v.Type == RemoteVolumeType.Files
                                          select v).Take(5).ToList();

                    var blockEntries = (from v in db.GetRemoteVolumes(tr)
                                        where v.Type == RemoteVolumeType.Blocks
                                        select v).Take(5).ToList();

                    using (var tempFileset = db.CreateTemporaryFileset(filesetEntries[0].ID, tr))
                    {
                        tempFileset.ApplyFilter(new FilterExpression("*.txt"));
                        tempFileset.ListAllDeletedFiles().Take(100).ToList();

                        var ids = tempFileset.ConvertToPermanentFileset("test-fileset", DateTime.UtcNow, false);
                    }

                    string name = db.GetRemoteVolumeNameForFileset(filesetEntries[0].ID, tr);
                    Assert.IsNotEmpty(name);

                    tr.Rollback();
                }
            }
        }

        [Test]
        [Category("Database")]
        public void Recreate()
        {
            using (var parentdb = new LocalDatabase(DBFILE, "database-test", true))
            {
                Options opt = new Options(new Dictionary<string, string>());
                Library.Main.Utility.UpdateOptionsFromDb(parentdb, opt);
                using (var db = new LocalRecreateDatabase(parentdb, opt))
                {
                    using (var tr = db.BeginTransaction())
                    {

                        var filesetEntries = (from v in db.GetRemoteVolumes(tr)
                                              where v.Type == RemoteVolumeType.Files
                                              select v).Take(5).ToList();

                        var blockEntries = (from v in db.GetRemoteVolumes(tr)
                                            where v.Type == RemoteVolumeType.Blocks
                                            select v).Take(5).ToList();
                        int hashsize = HashAlgorithmHelper.Create(opt.BlockHashAlgorithm).HashSize / 8;

                        db.FindMissingBlocklistHashes(hashsize, opt.Blocksize, tr);

                        long metaId = db.AddMetadataset("test-hash-meta", 10, null, 0, tr);
                        Assert.AreNotEqual(-1, metaId);
                        long blockId = db.AddBlockset("test-hash-block", 20, null, 0, tr);
                        Assert.AreNotEqual(-1, blockId);

                        string dirPath = "/test/path/";
                        var split = LocalDatabase.SplitIntoPrefixAndName(dirPath);
                        long prefixId = db.GetOrCreatePathPrefix(split.Key, tr);
                        db.AddDirectoryEntry(filesetEntries[0].ID, prefixId, split.Value, DateTime.UtcNow, metaId, tr);

                        string filePath = "/test/path/file.txt";
                        split = LocalDatabase.SplitIntoPrefixAndName(filePath);
                        prefixId = db.GetOrCreatePathPrefix(split.Key, tr);
                        db.AddFileEntry(filesetEntries[0].ID, prefixId, split.Value, DateTime.UtcNow, blockId, metaId, tr);

                        db.AddSmallBlocksetLink("test-hash-block", "test-hash-block", 20, tr);
                        // Insert
                        db.UpdateBlock("test-hash-meta", 20, blockEntries[0].ID, tr);
                        // Update
                        db.UpdateBlock("test-hash-meta", 40, blockEntries[0].ID, tr);

                        db.UpdateBlockset("test-hash-block", new[] { "test-hash-1" }, tr);
                        db.GetBlockLists(blockEntries[0].ID).Take(100).ToList();
                        db.GetMissingBlockListVolumes(1, opt.Blocksize, hashsize);
                        db.CleanupMissingVolumes();

                        tr.Rollback();
                    }
                }
            }
        }

        [Test]
        [Category("Database")]
        public void Repair()
        {
            using (var db = new LocalRepairDatabase(DBFILE))
            {
                List<RemoteVolumeEntry> filesetEntries;
                List<RemoteVolumeEntry> blockEntries;
                List<RemoteVolumeEntry> indexEntries;
                Options opt = new Options(new Dictionary<string, string>());
                using (var tr = db.BeginTransaction())
                {
                    Library.Main.Utility.UpdateOptionsFromDb(db, opt, tr);

                    filesetEntries = (from v in db.GetRemoteVolumes(tr)
                                      where v.Type == RemoteVolumeType.Files
                                      select v).Take(5).ToList();

                    blockEntries = (from v in db.GetRemoteVolumes(tr)
                                    where v.Type == RemoteVolumeType.Blocks
                                    select v).Take(5).ToList();

                    indexEntries = (from v in db.GetRemoteVolumes(tr)
                                    where v.Type == RemoteVolumeType.Index
                                    select v).Take(5).ToList();

                    tr.Rollback();
                }
                int hashsize = HashAlgorithmHelper.Create(opt.BlockHashAlgorithm).HashSize / 8;

                long filesetId = db.GetFilesetIdFromRemotename(filesetEntries[0].Name);
                Assert.AreEqual(filesetEntries[0].ID, filesetId);

                db.GetBlockVolumesFromIndexName(indexEntries[0].Name).Take(10).ToList();

                using (var missing = db.CreateBlockList(blockEntries[0].Name))
                {
                    missing.SetBlockRestored("test-block", 100);
                    missing.GetSourceFilesWithBlocks(10).Take(100).ToList();
                    missing.GetMissingBlocks().Take(100).ToList();
                    missing.GetFilesetsUsingMissingBlocks().Take(100).ToList();
                    missing.GetMissingBlockSources().Take(100).ToList();
                }

                db.FixDuplicateMetahash();
                db.FixDuplicateFileentries();
                db.FixMissingBlocklistHashes(opt.BlockHashAlgorithm, opt.Blocksize);
                try
                {
                    db.CheckAllBlocksAreInVolume(blockEntries[0].Name, new[] {
                            new KeyValuePair<string, long>("test-block", 100) });
                }
                catch { }
                try
                {
                    db.CheckBlocklistCorrect("test-block", 100, new[] { "test-block1", "test-block2" }, opt.Blocksize, hashsize);
                }
                catch { }
            }
        }

        [Test]
        [Category("Database")]
        public void Restore()
        {
            using (var db = new LocalRestoreDatabase(DBFILE))
            {
                List<RemoteVolumeEntry> filesetEntries;
                List<RemoteVolumeEntry> blockEntries;
                List<RemoteVolumeEntry> indexEntries;
                Options opt = new Options(new Dictionary<string, string>());
                using (var tr = db.BeginTransaction())
                {
                    Library.Main.Utility.UpdateOptionsFromDb(db, opt, tr);

                    filesetEntries = (from v in db.GetRemoteVolumes(tr)
                                      where v.Type == RemoteVolumeType.Files
                                      select v).Take(5).ToList();

                    blockEntries = (from v in db.GetRemoteVolumes(tr)
                                    where v.Type == RemoteVolumeType.Blocks
                                    select v).Take(5).ToList();

                    indexEntries = (from v in db.GetRemoteVolumes(tr)
                                    where v.Type == RemoteVolumeType.Index
                                    select v).Take(5).ToList();

                    tr.Rollback();
                }
                int hashsize = HashAlgorithmHelper.Create(opt.BlockHashAlgorithm).HashSize / 8;

                var restoreCount = db.PrepareRestoreFilelist(new DateTime(0), new[] { 0L }, new FilterExpression("*.txt"));
                Assert.NotZero(restoreCount.Item1);

                Assert.IsNotEmpty(db.GetFirstPath());
                string largestPrefix = db.GetLargestPrefix();
                Assert.IsNotEmpty(largestPrefix);

                db.SetTargetPaths("", "");
                db.FindMissingBlocks(false);
                db.GetMissingVolumes().Take(10).ToList();
                db.CreateProgressTracker(false);



                using (var marker = db.CreateBlockMarker())
                {
                    var existing = db.GetExistingFilesWithBlocks().Take(10).ToList();
                    long fileId = existing[0].TargetFileID;
                    //marker.SetAllBlocksMissing(fileId);

                    // TODO: db.GetMissingBlockData()

                    //marker.SetBlockRestored(fileId, 0, "test-hash", opt.Blocksize, false);
                    //marker.SetAllBlocksRestored(fileId, true);
                    //marker.SetFileDataVerified(fileId);
                    //marker.Commit();
                }

                db.GetTargetFolders().Take(10).ToList();
                db.GetFilesAndSourceBlocks(false, opt.Blocksize).Take(10).ToList();
                var fileList = db.GetFilesAndSourceBlocksFast(opt.Blocksize).Take(10).ToList();
                db.GetFilesToRestore(true).Take(10).ToList();
                db.UpdateTargetPath(0, "newName");
                db.DropRestoreTable();
            }
        }

        [Test]
        [Category("Database")]
        public void Test()
        {
            using (var db = new LocalTestDatabase(DBFILE))
            {
                List<RemoteVolumeEntry> filesetEntries;
                List<RemoteVolumeEntry> blockEntries;
                List<RemoteVolumeEntry> indexEntries;
                Options opt = new Options(new Dictionary<string, string>());
                using (var tr = db.BeginTransaction())
                {
                    Library.Main.Utility.UpdateOptionsFromDb(db, opt, tr);

                    filesetEntries = (from v in db.GetRemoteVolumes(tr)
                                      where v.Type == RemoteVolumeType.Files
                                      select v).Take(5).ToList();

                    blockEntries = (from v in db.GetRemoteVolumes(tr)
                                    where v.Type == RemoteVolumeType.Blocks
                                    select v).Take(5).ToList();

                    indexEntries = (from v in db.GetRemoteVolumes(tr)
                                    where v.Type == RemoteVolumeType.Index
                                    select v).Take(5).ToList();

                    tr.Rollback();
                }
                int hashsize = HashAlgorithmHelper.Create(opt.BlockHashAlgorithm).HashSize / 8;

                db.UpdateVerificationCount(filesetEntries[0].Name);

                int samples = 3;
                var testTargets = db.SelectTestTargets(samples, opt).ToList();
                Assert.IsNotEmpty(testTargets);

                using (var filelist = db.CreateFilelist(filesetEntries[0].Name))
                {
                    filelist.Add("path", 10, "hash", 20, "hash", null, FilelistEntryType.File, DateTime.Now);
                    Assert.IsNotEmpty(filelist.Compare().ToList());
                }
                using (var blocklist = db.CreateBlocklist(filesetEntries[0].Name))
                {
                    blocklist.AddBlock("hash", 10);
                    Assert.IsNotEmpty(blocklist.Compare().ToList());
                }
                using (var indexlist = db.CreateIndexlist(filesetEntries[0].Name))
                {
                    indexlist.AddBlockLink("path", "hash", 10);
                    Assert.IsNotEmpty(indexlist.Compare().ToList());
                }
            }
        }
    }
}
