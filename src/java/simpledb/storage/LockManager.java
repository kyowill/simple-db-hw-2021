package simpledb.storage;

import simpledb.common.Permissions;
import simpledb.transaction.TransactionId;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class LockManager {
    private ConcurrentHashMap<TransactionId, PageId> transactionOwnedMap;

    private ConcurrentHashMap<PageId, PageLock> pageLockMap;

    // one thread carry only one transaction, transaction is threadlocal of the thread
    class PageLock {
        final private PageId pageId;
        private Set<TransactionId> sharedSet;
        private TransactionId exclusive;
        private ReentrantReadWriteLock readWriteLock;

        public PageLock(PageId pid) {
            pageId = pid;
            readWriteLock = new ReentrantReadWriteLock();
            sharedSet = new HashSet<>();
        }

        public void lockShared(TransactionId tid) {
            Permissions permissions = holdLock(tid);
            if (permissions != null && permissions.equals(Permissions.READ_WRITE)) {
                return;
            }
            readWriteLock.readLock().lock();
            sharedSet.add(tid);
        }

        public void lockExclusive(TransactionId tid) {
            Permissions permissions = holdLock(tid);
            if (permissions != null && permissions.equals(Permissions.READ_ONLY) && sharedSet.size() == 1) {
                sharedSet.remove(tid);
                exclusive = tid;
                return;
            }
            readWriteLock.writeLock().lock();
            exclusive = tid;
        }

        public void unlock(TransactionId tid) {
            if (tid.equals(exclusive)) {
                readWriteLock.writeLock().unlock();
                exclusive = null;
            } else {
                readWriteLock.readLock().unlock();
                sharedSet.remove(tid);
            }
        }

        public Permissions holdLock(TransactionId transactionId) {
            if (transactionId.equals(exclusive)) {
                return Permissions.READ_WRITE;
            } else if (sharedSet.contains(transactionId)) {
                return Permissions.READ_ONLY;
            }
            return null;
        }
    }

    public LockManager() {
        transactionOwnedMap = new ConcurrentHashMap<>();
        pageLockMap = new ConcurrentHashMap<>();
    }

    public Permissions holdsLock(TransactionId tid, PageId p) {
        if (!transactionOwnedMap.containsKey(tid)) {
            return null;
        }
        if (!transactionOwnedMap.get(tid).equals(p)) {
            return null;
        }
        return pageLockMap.get(p).holdLock(tid);
    }

    public void unsafeReleasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        if (!transactionOwnedMap.containsKey(tid)) {
            return;
        }
        if (!transactionOwnedMap.get(tid).equals(pid)) {
            return;
        }

        // release shared lock or exclusive lock
        unlock(tid, pid);
    }


    public void lock(TransactionId tid, PageId pid, Permissions permissions) {
        if (!pageLockMap.containsKey(pid)) {
            synchronized (this) {
                if (!pageLockMap.containsKey(pid)) {
                    pageLockMap.put(pid, new PageLock(pid));
                }
            }
        }

        if (permissions.equals(Permissions.READ_ONLY)) {
            pageLockMap.get(pid).lockShared(tid);
        } else {
            pageLockMap.get(pid).lockExclusive(tid);
        }
        transactionOwnedMap.put(tid, pid);
    }

    public void unlock(TransactionId tid, PageId pid) {
        pageLockMap.get(pid).unlock(tid);
        transactionOwnedMap.remove(tid);
    }
}
