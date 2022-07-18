package simpledb.storage;

import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.transaction.Transaction;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;


public class LockManager {
    private ConcurrentHashMap<TransactionId, ConcurrentHashMap<PageId, Object>> transactionOwnedMap;

    private static Integer WAIT_SECONDS = 1;

    private ConcurrentHashMap<PageId, PageLock> pageLockMap;

    // one thread carry only one transaction, transaction is threadlocal of the thread
    class PageLock {
        final private PageId pageId;
        private final ConcurrentHashMap<TransactionId, Object> sharedSet;
        private volatile TransactionId exclusive;
        final private ReentrantLock sync;

        final private Condition condition;

        private volatile int status = 0; // 0 no lock, 1 shared lock, 2 exclusive lock

        public PageLock(PageId pid) {
            pageId = pid;
            sync = new ReentrantLock();
            condition = sync.newCondition();
            sharedSet = new ConcurrentHashMap<>();
            status = 0;
        }

        private int checkStatus() {
            return status;
        }

        private boolean isHoldBySelf(TransactionId tid) {
            if (tid.equals(exclusive)) {
                return true;
            }
            if (sharedSet.containsKey(tid)) {
                return true;
            }
            return false;
        }

        public void lockShared(TransactionId tid) throws TransactionAbortedException {
            // case 0 status为0 tid加锁成功, shared-set添加tid
            // case 1 status为1 tid加锁成功, shared-set添加tid
            // case 2 status为2 如果exclusive不是tid阻塞(当阻塞结束后，推出等待，检查状态如果是2，那么自然持有共享锁)，否则成功添加tid
            sync.lock();
            while (shouldWaitForExclusive(tid)) {
                try {
                    if (!condition.await(WAIT_SECONDS, TimeUnit.SECONDS)) {
                        throw new InterruptedException("wait too long");
                    }
                } catch (InterruptedException e) {
                    condition.signalAll();
                    sync.unlock();
                    throw new TransactionAbortedException(e);
                }
            }

            if (checkStatus() != 2) {
                sharedSet.putIfAbsent(tid, new Object());
                status = 1;
            }
            sync.unlock();
            // 如果 status == 2 独占锁本身持有共享锁
        }

        public void lockExclusive(TransactionId tid) throws TransactionAbortedException {
            // case 0 status 0 tid加锁成功 exclusive设置为tid
            // case 1 status 1 如果可以锁升级则不阻塞，否则阻塞
            // case 2 status 2 如果exclusive是tid则成功 否则阻塞

            sync.lock();
            int lockStatus = checkStatus();
            if (lockStatus == 2) {
                while (shouldWaitForExclusive(tid)) {
                    try {
                        if (!condition.await(WAIT_SECONDS, TimeUnit.SECONDS)) {
                            throw new InterruptedException("wait too long");
                        }
                    } catch (InterruptedException e) {
                        condition.signalAll();
                        sync.unlock();
                        throw new TransactionAbortedException(e);
                    }
                }
            } else if (lockStatus == 1) {
                while (!canUpgrade(tid)) {
                    try {
                        if (!condition.await(WAIT_SECONDS, TimeUnit.SECONDS)) {
                            throw new InterruptedException("wait too long");
                        }
                    } catch (InterruptedException e) {
                        condition.signalAll(); // notify other waiter
                        sync.unlock();
                        throw new TransactionAbortedException(e);
                    }
                }
                sharedSet.clear();
            }
            exclusive = tid;
            status = 2;
            sync.unlock();
        }

        private boolean canUpgrade(TransactionId tid) {
            if (checkStatus() == 1 && sharedSet.size() == 1 && sharedSet.containsKey(tid)) {
                return true;
            }
            return false;
        }

        private boolean shouldWaitForExclusive(TransactionId tid) {
            if (checkStatus() == 2 && exclusive != null && !exclusive.equals(tid)) {
                return true;
            }
            return false;
        }

        public void unlock(TransactionId tid) {
            // case 0 status为1 sharedSet删除tid
            // case 1 status为2 exclusive设置null
            // 处理 dead lock
            sync.lock();

            // dead lock abort
            if (!sharedSet.containsKey(tid) && !tid.equals(exclusive)) {
                sync.unlock();
                return;
            }

            if (sharedSet.isEmpty()) {
                // exclusive lock
                exclusive = null;
                status = 0;
            } else {
                // shared lock
                sharedSet.remove(tid);
                if (sharedSet.isEmpty()) {
                    status = 0;
                }
            }
            condition.signalAll();
            sync.unlock();
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
        if (!transactionOwnedMap.get(tid).contains(p)) {
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
        if (!transactionOwnedMap.get(tid).containsKey(pid)) {
            return;
        }

        // release shared lock or exclusive lock
        unlock(tid, pid);
    }


    public void lock(TransactionId tid, PageId pid, Permissions permissions) throws TransactionAbortedException {
        if (!pageLockMap.containsKey(pid)) {
            synchronized (this) {
                if (!pageLockMap.containsKey(pid)) {
                    pageLockMap.put(pid, new PageLock(pid));
                }
            }
        }
        if (!transactionOwnedMap.containsKey(tid)) {
            synchronized (this) {
                if (!transactionOwnedMap.containsKey(tid)) {
                    transactionOwnedMap.put(tid, new ConcurrentHashMap<>());
                }
            }
        }

        if (permissions.equals(Permissions.READ_ONLY)) {
            pageLockMap.get(pid).lockShared(tid);
        } else {
            pageLockMap.get(pid).lockExclusive(tid);
        }
        transactionOwnedMap.get(tid).put(pid, new Object());
    }

    public void unlock(TransactionId tid, PageId pid) {
        pageLockMap.get(pid).unlock(tid);
        transactionOwnedMap.get(tid).remove(pid);
        if (transactionOwnedMap.get(tid).isEmpty()) {
            synchronized (this) {
                if (transactionOwnedMap.get(tid).isEmpty()) {
                    transactionOwnedMap.remove(tid);
                }
            }
        }
    }

    public void releaseSharedPage(PageId pid) {
        pageLockMap.remove(pid);
    }

    public void releasePage(TransactionId tid) {
        // 如果是deadlock释放releasePage，tid不会持有任何page
        if (!transactionOwnedMap.containsKey(tid)) {
            return;
        }
        ConcurrentHashMap<PageId, Object> map = transactionOwnedMap.get(tid);
        for (Map.Entry<PageId, Object> entry : map.entrySet()) {
            unlock(tid, entry.getKey());
        }
    }
}
