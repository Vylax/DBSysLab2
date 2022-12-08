package simpledb;

import java.io.Serializable;

/**
 * A RecordId is a reference to a specific tuple on a specific page of a
 * specific table.
 */
public class RecordId implements Serializable {

    private static final long serialVersionUID = 1L;

    //CHANGES
    PageId pid;
    int tupleno;
    int hash;
    /**
     * Creates a new RecordId referring to the specified PageId and tuple
     * number.
     * 
     * @param pid
     *            the pageid of the page on which the tuple resides
     * @param tupleno
     *            the tuple number within the page.
     */
    public RecordId(PageId pid, int tupleno) {//CHANGES
        this.pid = pid;
        this.tupleno = tupleno;
        this.hash=Integer.parseInt(String.format("%s%s",pid.getPageNumber(),tupleno));
    }

    /**
     * @return the tuple number this RecordId references.
     */
    public int getTupleNumber() {//CHANGES
        return tupleno;
    }

    /**
     * @return the page id this RecordId references.
     */
    public PageId getPageId() {//CHANGES
        return pid;
    }

    /**
     * Two RecordId objects are considered equal if they represent the same
     * tuple.
     * 
     * @return True if this and o represent the same tuple
     */
    @Override
    public boolean equals(Object o) {//CHANGES
    	return o instanceof RecordId && this.equals((RecordId)o);
    }

    public boolean equals(RecordId rid) {//CHANGES
    	return rid==null ? this.pid==null : this.getPageId().equals(rid.getPageId()) && this.getTupleNumber()==rid.getTupleNumber();
    }

    /**
     * You should implement the hashCode() so that two equal RecordId instances
     * (with respect to equals()) have the same hashCode().
     * 
     * @return An int that is the same for equal RecordId objects.
     */
    @Override
    public int hashCode() {//CHANGES
        return hash;
    }

}
