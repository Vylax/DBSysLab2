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
    boolean isOpen;

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
        isOpen = false;
    }

    public Predicate getPredicate() {//CHANGES
        return p;
    }

    public TupleDesc getTupleDesc() {//CHANGES
        return child.getTupleDesc();
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {//CHANGES
        child.open();
        isOpen = true;
    }

    public void close() {//CHANGES
        child.close();
        isOpen = false;
    }

    public void rewind() throws DbException, TransactionAbortedException {//CHANGES
        if(!isOpen) throw new IllegalStateException("The iterator is not open");
        child.rewind();
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
        if(!isOpen) throw new IllegalStateException("The iterator is not open");

        while(child.hasNext()){
            Tuple nextCandidaTuple = child.next();
            if(p.filter(nextCandidaTuple)) return nextCandidaTuple;
        }
        return null;
    }

    @Override
    public OpIterator[] getChildren() {//CHANGES
        return new OpIterator[] {child};//TODO: check if this is what's expected
    }

    @Override
    public void setChildren(OpIterator[] children) {//CHANGES
        child = children[0];//TODO: check if this is what's expected
    }

}
