package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    //CHANGES
    private final int gbField;
    private final Type gbFieldType;
    private final int aField;
    private final Op operator;
    private final Map<Field, Integer> extremas, counts, sums, averages;
    private final Map<Op, Map<Field, Integer>> maps;
    private String gFieldName, aFieldName;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {//CHANGES
        this.gbField=gbfield;
        this.gbFieldType=gbfieldtype;
        this.aField=afield;
        this.operator=what;
        extremas = new HashMap<>(); // If the operator is min or max, it contains the min (resp max) value
        counts = new HashMap<>();
        sums = new HashMap<>();
        averages = new HashMap<>();

        maps = new HashMap<>();
        maps.put(Op.MAX, extremas);
        maps.put(Op.MIN, extremas);
        maps.put(Op.COUNT, counts);
        maps.put(Op.SUM, sums);
        maps.put(Op.AVG, averages);
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {//CHANGES
        Field groupField;
        Field aggField;

        if(gbField != Aggregator.NO_GROUPING) {
            groupField = tup.getField(gbField);
            gFieldName = tup.getTupleDesc().getFieldName(gbField);
        }
        else
            groupField = new IntField(Aggregator.NO_GROUPING);

        aggField = tup.getField(aField);
        aFieldName = tup.getTupleDesc().getFieldName(aField);

        if(operator == Op.COUNT || operator == Op.AVG) {
            if (!counts.containsKey(groupField))
                counts.put(groupField, 1);
            else
                counts.put(groupField, counts.get(groupField) + 1);
        }
        if(operator == Op.AVG || operator == Op.SUM) {
            if (!sums.containsKey(groupField))
                sums.put(groupField, aggField.hashCode());
            else
                sums.put(groupField, sums.get(groupField) + aggField.hashCode());
        }
        if(operator == Op.AVG)
            averages.put(groupField, sums.get(groupField) / counts.get(groupField));

        if(operator == Op.MAX || operator == Op.MIN){
            if(!extremas.containsKey(groupField))
                extremas.put(groupField, aggField.hashCode());
            else
                extremas.put(groupField, operator == Op.MAX ? Math.max(aggField.hashCode(), extremas.get(groupField)) : Math.min(aggField.hashCode(), extremas.get(groupField)));
        }
        else if (operator != Op.COUNT && operator != Op.SUM && operator != Op.AVG) {
            throw new UnsupportedOperationException("Operation not supported");
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {//CHANGES
        TupleDesc desc = getTupleDesc();
        List<Tuple> tuples = convertToTuples(maps.get(operator), desc);

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