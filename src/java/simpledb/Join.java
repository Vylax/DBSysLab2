package simpledb;

import java.util.*;

/**
 * The Join operator implements the relational join operation.
 */
public class Join extends Operator {

    private static final long serialVersionUID = 1L;

    //CHANGES
    JoinPredicate p;
    OpIterator child1;
    OpIterator child2;
    TupleDesc td;
    Tuple tuple1;

    /**
     * Constructor. Accepts two children to join and the predicate to join them
     * on
     * 
     * @param p
     *            The predicate to use to join the children
     * @param child1
     *            Iterator for the left(outer) relation to join
     * @param child2
     *            Iterator for the right(inner) relation to join
     */
    public Join(JoinPredicate p, OpIterator child1, OpIterator child2) {//CHANGES
        this.p = p;
        this.child1 = child1;
        this.child2 = child2;
        td = null;
    }

    public JoinPredicate getJoinPredicate() {//CHANGES
        return p;
    }

    /**
     * @return
     *       the field name of join field1. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField1Name() {//CHANGES
        return child1.getTupleDesc().getFieldName(p.getField1());//TODO Should be quantified by alias or table name. ??????
    }

    /**
     * @return
     *       the field name of join field2. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField2Name() {//CHANGES
        return child2.getTupleDesc().getFieldName(p.getField2());//TODO Should be quantified by alias or table name. ??????
    }

    /**
     * @see simpledb.TupleDesc#merge(TupleDesc, TupleDesc) for possible
     *      implementation logic.
     */
    public TupleDesc getTupleDesc() {//CHANGES
        if(td == null) td = TupleDesc.merge(child1.getTupleDesc(), child2.getTupleDesc());
        return td;
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {//CHANGES
    	super.open();
        child1.open();
        child2.open();
    }

    public void close() {//CHANGES
        super.close();
        child1.close();
        child2.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {//CHANGES
        child1.rewind();
        child2.rewind();
    }

    /**
     * Returns the next tuple generated by the join, or null if there are no
     * more tuples. Logically, this is the next tuple in r1 cross r2 that
     * satisfies the join predicate. There are many possible implementations;
     * the simplest is a nested loops join.
     * <p>
     * Note that the tuples returned from this particular implementation of Join
     * are simply the concatenation of joining tuples from the left and right
     * relation. Therefore, if an equality predicate is used there will be two
     * copies of the join attribute in the results. (Removing such duplicate
     * columns can be done with an additional projection operator if needed.)
     * <p>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     * 
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {//CHANGES
    	if(tuple1 == null){
    		if(!child1.hasNext()) 
    			return null;
    		tuple1 = child1.next();
	    }
    	
	    while(tuple1 != null) {
		    while (child2.hasNext()) {
			    Tuple tuple2 = child2.next();
			    
			    if (getJoinPredicate().filter(tuple1, tuple2)) {
				    Tuple tuple = new Tuple(getTupleDesc());
				    
				    int i = 0;
				    Iterator<Field> fields = tuple1.fields();
				    
				    while (fields.hasNext()) {
					    tuple.setField(i++, fields.next());
				    }
				    
				    Iterator<Field> fields1 = tuple2.fields();
				    
				    while (fields1.hasNext()) {
					    tuple.setField(i++, fields1.next());
				    }
				    
				    return tuple;
			    }
		    }
		    
		    child2.rewind();
		    if(!child1.hasNext())
		    	break;
		    
		    tuple1 = child1.next();
	    }
	    return null;
    }

    @Override
    public OpIterator[] getChildren() {//CHANGES
        return new OpIterator[] {child1, child2};
    }

    @Override
    public void setChildren(OpIterator[] children) {//CHANGES
       if(children.length != 2) throw new IllegalArgumentException("Two children are expected, got " + children.length);

       child1 = children[0];
       child2 = children[1];
    }

}
