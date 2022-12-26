package simpledb;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;

    //CHANGES
    Predicate p;
    OpIterator child;

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     * 
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */
    public Filter(Predicate p, OpIterator child) {//CHANGES
        this.p = p;
        this.child = child;
    }

    public Predicate getPredicate() {//CHANGES
        return p;
    }

    public TupleDesc getTupleDesc() {//CHANGES
        return child.getTupleDesc();
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {//CHANGES
        // some code goes here
    }

    public void close() {//CHANGES
        // some code goes here
    }

    public void rewind() throws DbException, TransactionAbortedException {//CHANGES
        // some code goes here
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     * 
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {//CHANGES
        // some code goes here
        return null;
    }

    @Override
    public OpIterator[] getChildren() {//CHANGES
        // some code goes here
        return null;
    }

    @Override
    public void setChildren(OpIterator[] children) {//CHANGES
        // some code goes here
    }

}
