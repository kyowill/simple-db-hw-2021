package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @author Sam Madden
 * @see HeapPage#HeapPage
 */
public class HeapFile implements DbFile {

    private File file;
    private TupleDesc tupleDesc;

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap
     *          file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        file = f;
        tupleDesc = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        // throw new UnsupportedOperationException("implement this");
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        // throw new UnsupportedOperationException("implement this");
        return tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        HeapPage page = null;
        RandomAccessFile seeker = null;
        try {
            seeker = new RandomAccessFile(file, "r"); // read only
            int pageNo = pid.getPageNumber();
            int offset = pageNo * BufferPool.getPageSize();
            seeker.seek(offset);
            byte[] pageBytes = new byte[BufferPool.getPageSize()]; // one page bytes
            seeker.read(pageBytes);
            HeapPageId heapPageId = new HeapPageId(pid.getTableId(), pid.getPageNumber());
            page = new HeapPage(heapPageId, pageBytes);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                seeker.close();
            } catch (IOException e) {
                throw new RuntimeException("close failed");
            }
        }
        return page;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        long len = file.length();
        int page = BufferPool.getPageSize();
        int num = (int) (len / page);
        if (len % page == 0) {
            return num;
        }
        return num + 1;
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new DbFileItr(tid);
    }

    private class DbFileItr implements DbFileIterator {

        private boolean opened;

        private int pageCursor;

        private Iterator<Tuple> curItr;

        private TransactionId tid;

        public DbFileItr(TransactionId tid) {
            opened = false;
            this.tid = tid;
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            opened = true;
            pageCursor = 0;
            HeapPageId pageId = new HeapPageId(getId(), pageCursor);
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_ONLY);
            curItr = page.iterator();
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (!opened) {
                return false;
            }
            if (!curItr.hasNext() && pageCursor == numPages() - 1) {
                return false;
            }
            if (!curItr.hasNext()) {
                pageCursor++;
                HeapPageId pageId = new HeapPageId(getId(), pageCursor);
                HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_ONLY);
                curItr = page.iterator();
                ;
            }
            return true;
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (!opened) {
                throw new NoSuchElementException("not opened");
            }
            if (!hasNext()) {
                throw new NoSuchElementException("not opened");
            }
            return curItr.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            pageCursor = 0;
            HeapPageId pageId = new HeapPageId(getId(), pageCursor);
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_ONLY);
            curItr = page.iterator();
        }

        @Override
        public void close() {
            opened = false;
        }
    }

}

