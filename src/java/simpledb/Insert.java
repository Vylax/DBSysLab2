package simpledb;

import java.io.IOException;
import java.util.Random;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    //CHANGES
    OpIterator child;
    int tableId;
    TupleDesc td;
    private boolean wasCalled = false;
    TransactionId t;


    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId) throws DbException { //CHANGES
        this.child = child;
        this.tableId = tableId;
        this.td = new TupleDesc(new Type[] { Type.INT_TYPE });
        this.t = t; // Is needed for the fetchnext method
    }

    public TupleDesc getTupleDesc() { //CHANGES
        return td;
    }

    public void open() throws DbException, TransactionAbortedException { //CHANGES
        super.open();
        child.open();
    }

    public void close() { //CHANGES
        super.close();
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException { //CHANGES
        child.rewind();
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
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException { //CHANGES
        if(wasCalled)
            return null;
        wasCalled = true;

        int count = 0;
        while (child.hasNext()) {
            try {
                Database.getBufferPool().insertTuple(t, tableId, child.next());
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            count++;
        }
        Tuple output = new Tuple(td);
        output.setField(0,new IntField(count));
        return output;
    }

    @Override
    public OpIterator[] getChildren() { //CHANGES
        return new OpIterator[]{ child };
    }

    @Override
    public void setChildren(OpIterator[] children) { //CHANGES
        child=children[0];
    }
}
