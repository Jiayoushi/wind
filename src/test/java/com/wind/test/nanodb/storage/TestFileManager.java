package com.wind.test.nanodb.storage;


import java.io.File;
import java.io.IOException;

import org.junit.Before;

import com.wind.nanodb.storage.BufferManager;
import com.wind.nanodb.storage.DBFile;
import com.wind.nanodb.storage.DBFileType;
import com.wind.nanodb.storage.DBPage;
import com.wind.nanodb.storage.FileManager;
import com.wind.nanodb.storage.FileManagerImpl;


/**
 *
 */
public class TestFileManager extends StorageTestCase {

    private FileManager fileMgr;

    private BufferManager bufMgr;


    @Before
    public void beforeClass() {
        fileMgr = new FileManagerImpl(testBaseDir);
        bufMgr = new BufferManager(null, fileMgr);
    }


    public void testCreateDeleteFile() throws IOException {
        String filename = "TestFileManager_testCreateDeleteFile";
        File f = new File(testBaseDir, filename);
        if (f.exists())
            f.delete();

        DBFile dbf = fileMgr.createDBFile(filename, DBFileType.HEAP_TUPLE_FILE,
            DBFile.DEFAULT_PAGESIZE);

        f = dbf.getDataFile();
        assert f.getName().equals(filename);
        assert f.length() == DBFile.DEFAULT_PAGESIZE;
        assert f.canRead();

        DBPage page0 = new DBPage(bufMgr, dbf, 0);
        fileMgr.loadPage(dbf, 0, page0.getPageData());

        assert page0.readByte(0) == DBFileType.HEAP_TUPLE_FILE.getID();
        assert DBFile.decodePageSize(page0.readByte(1)) == DBFile.DEFAULT_PAGESIZE;

        fileMgr.deleteDBFile(dbf);
        assert !f.exists();
    }


    public void testDoubleCreateFile() throws IOException {
        String filename = "TestFileManager_testDoubleCreateFile";
        File f = new File(testBaseDir, filename);
        if (f.exists())
            f.delete();

        DBFile dbf = fileMgr.createDBFile(filename, DBFileType.HEAP_TUPLE_FILE,
            DBFile.DEFAULT_PAGESIZE);

        f = dbf.getDataFile();
        assert f.getName().equals(filename);
        assert f.length() == DBFile.DEFAULT_PAGESIZE;
        assert f.canRead();

        try {
            DBFile dbf2 = fileMgr.createDBFile(filename,
                DBFileType.HEAP_TUPLE_FILE, DBFile.DEFAULT_PAGESIZE);

            assert false : "Shouldn't be able to create a DBFile twice.";
        }
        catch (IOException e) {
            // Success.
        }

        fileMgr.deleteDBFile(dbf);
        assert !f.exists();
    }
}