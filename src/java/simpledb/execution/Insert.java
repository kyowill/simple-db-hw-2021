package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId tid;

    private OpIterator child;

    private int tableId;

    private Iterator<Tuple> resultTupleItr;

    /**
     * Constructor.
     *
     * @param t       The transaction running the insert.
     * @param child   The child operator from which to read tuples to be inserted.
     * @param tableId The table in which to insert tuples.
     * @throws DbException if TupleDesc of child differs from table into which we are to
     *                     insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        // some code goes here
        tid = t;
        this.child = child;
        this.tableId = tableId;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return new TupleDesc(new Type[]{Type.INT_TYPE});
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        super.open();
        child.open();
        int count = 0;
        while (child.hasNext()) {
            Tuple next = child.next();
            try {
                Database.getBufferPool().insertTuple(tid, tableId, next);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            count++;
        }
        List<Tuple> oneList = new ArrayList<>();
        Tuple result = new Tuple(getTupleDesc());
        result.setField(0, new IntField(count));
        oneList.add(result);
        resultTupleItr = oneList.iterator();
    }

    public void close() {
        // some code goes here
        child.close();
        super.close();
        resultTupleItr = null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        child.rewind();
        open();
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     * null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (resultTupleItr.hasNext()) {
            return resultTupleItr.next();
        }
        return null;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        OpIterator[] opIterators = new OpIterator[1];
        opIterators[0] = child;
        return opIterators;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        child = children[0];
    }
}
