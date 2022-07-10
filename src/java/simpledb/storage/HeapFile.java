package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

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
        try {
            RandomAccessFile seeker = new RandomAccessFile(file, "r"); // read only
            int pageNo = pid.getPageNumber();
            int offset = pageNo * BufferPool.getPageSize();
            seeker.seek(offset);
            byte[] pageBytes = new byte[BufferPool.getPageSize()]; // one page bytes
            seeker.read(pageBytes);
            seeker.close();
            HeapPageId heapPageId = new HeapPageId(pid.getTableId(), pid.getPageNumber());
            page = new HeapPage(heapPageId, pageBytes);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return page;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        try {
            RandomAccessFile seeker = new RandomAccessFile(file, "rw"); // read only
            int pageNo = page.getId().getPageNumber();
            int offset = pageNo * BufferPool.getPageSize();
            seeker.seek(offset);
            seeker.write(page.getPageData());
            seeker.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return calFileSize(file);
    }

    private int calFileSize(File file) {
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
        // not necessary for lab1
        List<Page> result = new ArrayList<>();
        // 插入时并不知道tuple属于哪个page，所有要先找到一个可用的page
        HeapPage writtenPage = null;
        HeapPageId heapPageId = new HeapPageId(this.getId(), numPages() - 1);
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, heapPageId, Permissions.READ_ONLY);
        Database.getBufferPool().unsafeReleasePage(tid, heapPageId);
        if (page.getNumEmptySlots() == 0) {
            synchronized (this) {
                if (page.getNumEmptySlots() == 0) {
                    HeapPageId nPageId = new HeapPageId(this.getId(), numPages());
                    // HeapPage heapPage = new HeapPage(nPageId, new byte[BufferPool.getPageSize()]);
                    HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid, nPageId, Permissions.READ_WRITE);
                    this.writePage(heapPage); // must flush for right page number
                    writtenPage = heapPage;
                }
            }
        } else {
            page = (HeapPage) Database.getBufferPool().getPage(tid, heapPageId, Permissions.READ_WRITE);
            writtenPage = page;
        }
        writtenPage.insertTuple(t);
        result.add(writtenPage);
        return result;
    }

    // see DbFile.java for javadocs
    public List<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        List<Page> result = new ArrayList<>();
        PageId pageId = t.getRecordId().getPageId();
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_WRITE);
        page.deleteTuple(t);
        result.add(page);
        return result;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new DbFileItr(tid);
    }

    private class DbFileItr implements DbFileIterator {

        private boolean opened;

        private int pageCursor;

        private int size;

        private Iterator<Tuple> curItr;

        private TransactionId tid;

        public DbFileItr(TransactionId tid) {
            opened = false;
            this.tid = tid;
            size = numPages();
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
            if (!curItr.hasNext() && pageCursor == size - 1) {
                return false;
            }
            while ((!curItr.hasNext()) && pageCursor < size - 1) {

                Database.getBufferPool().unsafeReleasePage(tid, new HeapPageId(getId(), pageCursor));
                pageCursor++;
                HeapPageId pageId = new HeapPageId(getId(), pageCursor);
                HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_ONLY);
                curItr = page.iterator();
            }
            return curItr.hasNext();
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (!opened) {
                throw new NoSuchElementException("not opened");
            }
            if (!hasNext()) {
                throw new NoSuchElementException("no more tuples");
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

