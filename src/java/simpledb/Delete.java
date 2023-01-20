package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    OpIterator child;
    TupleDesc td;
    private boolean wasCalled = false;
    TransactionId t;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) { //CHANGES
        this.child = child;
        this.td = new TupleDesc(new Type[] { Type.INT_TYPE });
        this.t = t;
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

    public void rewind() throws DbException, TransactionAbortedException {
        child.rewind();
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException { //CHANGES
        if(wasCalled)
            return null;
        wasCalled = true;

        int count = 0;
        while (child.hasNext()) {
            try {
                Database.getBufferPool().deleteTuple(t, child.next());
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
