package simpledb.storage;

import simpledb.common.*;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 *
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /**
     * Bytes per page, including header.
     */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;

    /**
     * Default number of pages passed to the constructor. This is used by
     * other classes. BufferPool should use the numPages argument to the
     * constructor instead.
     */
    public static final int DEFAULT_PAGES = 50;

    private int capacity;

    ConcurrentHashMap<PageId, Page> bufferPool;

    // 与bufferpool数据可能不一致，evict时只是不清除dirtyPage，有共享锁的page可以被清除
    ConcurrentHashMap<TransactionId, ConcurrentHashMap<PageId, Object>> transactionOwnedPages;

    private LockManager lockManager;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        capacity = numPages;
        bufferPool = new ConcurrentHashMap<>();
        transactionOwnedPages = new ConcurrentHashMap<>();
        lockManager = new LockManager();
    }

    public static int getPageSize() {
        return pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
        BufferPool.pageSize = pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
        BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid  the ID of the transaction requesting the page
     * @param pid  the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {
        // some code goes here
        lockManager.lock(tid, pid, perm);
        if (!bufferPool.containsKey(pid)) {
            synchronized (this) {
                if (bufferPool.containsKey(pid)) {
                    return bufferPool.get(pid);
                }
                if (bufferPool.size() == capacity) {
                    evictPage();
                }
                int tableId = pid.getTableId();
                DbFile file = Database.getCatalog().getDatabaseFile(tableId);
                Page page = file.readPage(pid);
                bufferPool.put(pid, page);
            }
        }
        //
        if (!transactionOwnedPages.containsKey(tid)) {
            synchronized (this) {
                if (!transactionOwnedPages.containsKey(tid)) {
                    transactionOwnedPages.put(tid, new ConcurrentHashMap<>());
                }
            }
        }
        transactionOwnedPages.get(tid).put(pid, new Object());
        return bufferPool.get(pid);
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public void unsafeReleasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        lockManager.unsafeReleasePage(tid, pid);
        transactionOwnedPages.get(tid).remove(pid);
        if (transactionOwnedPages.get(tid).isEmpty()) {
            transactionOwnedPages.remove(tid);
        }
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // some code goes here
        // not necessary for lab1|lab2
        transactionComplete(tid, true);
    }

    /**
     * Return true if the specified transaction has a lock on the specified page
     */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return lockManager.holdsLock(tid, p) != null;
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid    the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        // some code goes here
        // not necessary for lab1|lab2
        if (commit) {
            try {
                flushPages(tid);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            if (transactionOwnedPages.containsKey(tid)) {
                synchronized (this) {
                    for (Map.Entry<PageId, Object> entry : transactionOwnedPages.get(tid).entrySet()) {
                        PageId pid = entry.getKey();
                        if (!bufferPool.containsKey(pid)) {
                            continue;
                        }
                        Page page = bufferPool.get(entry.getKey());
                        TransactionId dirTid = page.isDirty();
                        if (dirTid != null && dirTid.equals(tid)) {
                            page.markDirty(false, null);
                        }
                    }
                }
                transactionOwnedPages.remove(tid);
            }
        }
        lockManager.releasePage(tid);
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other
     * pages that are updated (Lock acquisition is not needed for lab2).
     * May block if the lock(s) cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid     the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t       the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        List<Page> pageList = Database.getCatalog().getDatabaseFile(tableId).insertTuple(tid, t);
        for (Page page : pageList) {
            PageId pid = page.getId();
            page.markDirty(true, tid);
            synchronized (this) {
                if (bufferPool.size() == capacity) {
                    evictPage();
                }
                bufferPool.put(pid, page); // bufferpool里原来的page可能被删掉
            }
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction deleting the tuple.
     * @param t   the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        int tableId = t.getRecordId().getPageId().getTableId();
        List<Page> pageList = Database.getCatalog().getDatabaseFile(tableId).deleteTuple(tid, t);
        for (Page page : pageList) {
            page.markDirty(true, tid);
            PageId pid = page.getId();
            synchronized (this) {
                if (bufferPool.size() == capacity) {
                    evictPage();
                }
                bufferPool.put(pid, page); // bufferpool里原来的page可能被删掉
            }
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     * break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        for (Map.Entry<PageId, Page> entry : bufferPool.entrySet()) {
            PageId pageId = entry.getKey();
            flushPage(pageId);
        }
    }

    /**
     * Remove the specific page id from the buffer pool.
     * Needed by the recovery manager to ensure that the
     * buffer pool doesn't keep a rolled back page in its
     * cache.
     * <p>
     * Also used by B+ tree files to ensure that deleted pages
     * are removed from the cache so they can be reused safely
     */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
        bufferPool.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     *
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        Page page = bufferPool.get(pid);
        if (page.isDirty() != null) {
            Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(page);
            page.markDirty(false, null);
        }
    }

    /**
     * Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        if (!transactionOwnedPages.containsKey(tid)) {
            return;
        }
        for (Map.Entry<PageId, Object> entry : transactionOwnedPages.get(tid).entrySet()) {
            PageId pid = entry.getKey();
            if (bufferPool.containsKey(pid)) {
                Page page = bufferPool.get(pid);
                TransactionId dirTid = page.isDirty();
                if (dirTid != null && dirTid.equals(tid)) {
                    flushPage(page.getId());
                }
            }
        }
        transactionOwnedPages.remove(tid);
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
        for (Map.Entry<PageId, Page> entry : bufferPool.entrySet()) {
            PageId pageId = entry.getKey();
            Page page = entry.getValue();
            TransactionId tid = page.isDirty();
            if (tid != null) {
                continue;
            }
            // lockManager.releaseSharedPage(pageId);
            bufferPool.remove(pageId);
            return;
        }
        throw new DbException("all pages is dirty, evict failed");
    }

}
