package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    //CHANGES
    private final int gbField;
    private final Type gbFieldType;
    private final int aField;
    private final Map<Field, Integer> counts;
    private String gFieldName, aFieldName;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {//CHANGES
        // Check if the operator is COUNT, if not, throw an error
        if(what != Op.COUNT)
            throw new IllegalArgumentException("Operation not supported"); 

        this.gbField=gbfield;
        this.gbFieldType=gbfieldtype;
        this.aField=afield;
        counts = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {//CHANGES
        Field groupField;

        if(gbField != Aggregator.NO_GROUPING) {
            groupField = tup.getField(gbField);
            gFieldName = tup.getTupleDesc().getFieldName(gbField);
        }
        else
            groupField = new IntField(Aggregator.NO_GROUPING);

        aFieldName = tup.getTupleDesc().getFieldName(aField);

        if (!counts.containsKey(groupField))
            counts.put(groupField, 1);
        else
            counts.put(groupField, counts.get(groupField) + 1);
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {//CHANGES
        TupleDesc desc = getTupleDesc();
        List<Tuple> tuples = convertToTuples(counts, desc);

        return new TupleIterator(desc, tuples);
    }

    //CHANGES
    private TupleDesc getTupleDesc(){
        Type[] typeAr;
        String[] fieldAr;

        boolean no_grouping = gbField == Aggregator.NO_GROUPING;

        typeAr = no_grouping ? new Type[] { Type.INT_TYPE } : new Type[] { gbFieldType, Type.INT_TYPE };
        fieldAr = no_grouping ? new String[] { aFieldName } : new String[] { gFieldName, aFieldName };
        
        return new TupleDesc(typeAr, fieldAr);
    }

    //CHANGES
    private List<Tuple> convertToTuples(Map<Field, Integer> values, TupleDesc desc) {
        List<Tuple> tuples = new ArrayList<>();
        for(Map.Entry<Field, Integer> entry: values.entrySet()){
            Tuple tuple = new Tuple(desc);
            if(gbField == Aggregator.NO_GROUPING){
                tuple.setField(0, new IntField(entry.getValue()));
            }
            else {
                tuple.setField(0, entry.getKey());
                tuple.setField(1, new IntField(entry.getValue()));
            }
            tuples.add(tuple);
        }
        return tuples;
    }
}
