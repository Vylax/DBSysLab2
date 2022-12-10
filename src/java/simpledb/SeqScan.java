package simpledb;

import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements OpIterator {

    private static final long serialVersionUID = 1L;
    //CHANGES
    TupleDesc td;
    int tableid;
    String tableAlias;
    TransactionId tid;
    DbFileIterator iterator;

    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid
     *            The transaction this scan is running as a part of.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {//CHANGES
        this.tid = tid;
        reset(tableid, tableAlias);
    }

    //CHANGES
    private void resetTD(int tableid, String tableAlias){
        TupleDesc originalTD = Database.getCatalog().getTupleDesc(tableid);
        Type[] tdTypes = new Type[originalTD.numFields()];
        String[] tdFieldNames = new String[originalTD.numFields()];
        for(int i=0; i<originalTD.numFields(); i++) {
            tdTypes[i] = originalTD.getFieldType(i);
            tdFieldNames[i] = tableAlias + "." + originalTD.getFieldName(i);
        }
        this.td = new TupleDesc(tdTypes,tdFieldNames);
    }

    /**
     * @return
     *       return the table name of the table the operator scans. This should
     *       be the actual name of the table in the catalog of the database
     * */
    public String getTableName() {//CHANGES
        return Database.getCatalog().getTableName(tableid);
    }

    /**
     * @return Return the alias of the table this operator scans.
     * */
    public String getAlias()//CHANGES
    {
        return tableAlias;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public void reset(int tableid, String tableAlias) {//CHANGES
        this.tableid = tableid;
        this.tableAlias = tableAlias;
        resetTD(tableid, tableAlias);
        this.iterator = Database.getCatalog().getDatabaseFile(tableid).iterator(tid);
    }

    public SeqScan(TransactionId tid, int tableId) {
        this(tid, tableId, Database.getCatalog().getTableName(tableId));
    }

    public void open() throws DbException, TransactionAbortedException {//CHANGES
        iterator.open();
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.  The alias and name should be separated with a "." character
     * (e.g., "alias.fieldName").
     *
     * @return the TupleDesc with field names from the underlying HeapFile,
     *         prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {//CHANGES
        return td;
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {//CHANGES
        return iterator.hasNext();
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {//CHANGES
        return iterator.next();
    }

    public void close() {//CHANGES
        iterator.close();
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {//CHANGES
        iterator.rewind();
    }
}
