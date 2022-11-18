package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {//CHANGES
        return Arrays.asList(TDItems).iterator();
    }

    private static final long serialVersionUID = 1L;
    TDItem[] TDItems;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {//CHANGES
    	TDItems = new TDItem[typeAr.length];
    	
    	for (int i=0; i < TDItems.length; i++) {
    		TDItems[i] = new TDItem(typeAr[i],fieldAr[i]);
    	}
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {//CHANGES
    	TDItems = new TDItem[typeAr.length];
    	
    	for (int i=0; i < TDItems.length; i++) {
    		TDItems[i] = new TDItem(typeAr[i],null);
    	}
    }
    
    public TupleDesc(TDItem[] TDItems) {//CHANGES
    	this.TDItems = TDItems;
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {//CHANGES
        return TDItems.length;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {//CHANGES
    	if(i < 0 || i >= numFields()) {
    		throw new NoSuchElementException("Index out of Range!");
    	}
        return TDItems[i].fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {//CHANGES
    	if(i < 0 || i >= numFields()) {
    		throw new NoSuchElementException("Index out of Range!");
    	}
        return TDItems[i].fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {//CHANGES
    	for (int i=0; i < numFields(); i++) {
    		if(name==null || getFieldName(i)==null) {
    			if(name==getFieldName(i)) {
    				return i;
    			}
    		}
    		else if(name.equals(getFieldName(i))) {
    			return i;
    		}
    	}
    	
    	throw new NoSuchElementException("No field named "+name+" in TupleDesc!");
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {//CHANGES
    	List<TDItem> temp = Arrays.asList(TDItems);
    	int sum=0;
    	for(int i=0; i<this.numFields();i++) {
    		sum += temp.get(i).fieldType.getLen() /*+ Type.STRING_LEN*/;//TODO is this right ? I'm not sure about the description of the method
    	}
        return sum;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {//CHANGES
    	TDItem[] newTD = new TDItem[td1.numFields()+td2.numFields()];

    	Iterator<TDItem> it1 = td1.iterator();
    	Iterator<TDItem> it2 = td2.iterator();
    	
    	for(int i=0; i<newTD.length; i++) {
    		newTD[i] = i < td1.numFields() ? it1.next() : it2.next();
    	}
        return new TupleDesc(newTD);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {//CHANGES
        return this == o;
    }
    
    public boolean equals(TupleDesc td) {//CHANGES
    	if(td == null || this == null) {
    		return td==this;
    	}
        if(this.numFields() != td.numFields()) {
        	return false;
        }
        boolean temp = true;
        
        for(int i=0;i<this.numFields();i++) {
        	temp &= this.getFieldType(i)==td.getFieldType(i);
        }
        return temp;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        String temp = "";
        for(int i=0;i<TDItems.length; i++) {
        	temp += String.format("%s(%s)", TDItems[i].fieldType, (TDItems[i].fieldName != null ? TDItems[i].fieldName : "")) + (i<TDItems.length-1 ? ", " : "");
        }
        return temp;
    }
}
