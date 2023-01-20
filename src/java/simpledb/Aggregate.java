package simpledb;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    //CHANGES
    private Aggregator aggregator;
    private OpIterator child;
    private final int aField;
    private final int gField;
    private final Aggregator.Op aop;
    private OpIterator iterator;
    /**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     * 
     * 
     * @param child
     *            The OpIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     */
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {//CHANGES
        this.child = child;
        this.aField = afield;
        this.gField = gfield;
        this.aop = aop;

        TupleDesc TD = child.getTupleDesc();
        Type aType = TD.getFieldType(afield);

        if(aType == Type.INT_TYPE || aType == Type.STRING_TYPE) {
            Type type = gfield == -1 ? null : TD.getFieldType(gField);
            aggregator = aType == Type.INT_TYPE ? new IntegerAggregator(gfield, type, afield, aop)
                                                : new StringAggregator(gfield, type, afield, aop);
        }
        iterator = aggregator.iterator();
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {//CHANGES
	    return gField;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     *         null;
     * */
    public String groupFieldName() { //CHANGES
        return gField == Aggregator.NO_GROUPING ? null : child.getTupleDesc().getFieldName(gField);
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() { //CHANGES
        return aField;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() { //CHANGES
        return child.getTupleDesc().getFieldName(aField);
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() { //CHANGES
        return aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
	    return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException, TransactionAbortedException { //CHANGES
        super.open();
        child.open();

        while(child.hasNext()) {
            aggregator.mergeTupleIntoGroup(child.next());
        }

        iterator = aggregator.iterator();
        iterator.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException { //CHANGES
        return iterator.hasNext() ? iterator.next() : null;
    }

    public void rewind() throws DbException, TransactionAbortedException { //CHANGES
        iterator.rewind();
        child.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * 
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() { //CHANGES
        Type aType = child.getTupleDesc().getFieldType(aField);
        String aName = child.getTupleDesc().getFieldName(aField);
        aName = aName != null ? nameOfAggregatorOp(aop) + "(" + aName + ")" : null;

        boolean no_grouping = aField == Aggregator.NO_GROUPING;

        Type[] fieldType = no_grouping ? new Type[] { aType } : new Type[] { child.getTupleDesc().getFieldType(gField), aType };
        String[] fieldName = no_grouping ? new String[] { aName } : new String[] { child.getTupleDesc().getFieldName(gField), aName };
        
        return new TupleDesc(fieldType, fieldName);
    }

    public void close() { //CHANGES
        super.close();
        iterator.close();
        child.close();
    }

    @Override
    public OpIterator[] getChildren() { //CHANGES
        return new OpIterator[] { child };
    }

    @Override
    public void setChildren(OpIterator[] children) { //CHANGES
        child = children[0];
    }
    
}
