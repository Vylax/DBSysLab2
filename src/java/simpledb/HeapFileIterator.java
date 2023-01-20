package simpledb;

import java.io.File;
import java.util.*;

/**
 * Implements a DbFileIterator by wrapping an Iterable<Tuple>.
 */
public class HeapFileIterator implements DbFileIterator {
	
	int pagesCount;
	int id;
	TransactionId tid;
	
	Iterator<Tuple> i;
	int currPageNumber;
	HeapPage hp;
	HeapPageId hpid;

    public HeapFileIterator(int pagesCount, int id, TransactionId tid) {
        this.pagesCount = pagesCount;
        this.id = id;
        this.tid = tid;
    }

    @Override
    public void open() throws TransactionAbortedException, DbException {
    	currPageNumber = 0;
        hpid = new HeapPageId(id, currPageNumber);
        hp = (HeapPage)Database.getBufferPool().getPage(tid, hpid, null);
        i = hp.iterator();
    }

    @Override
    public boolean hasNext() throws TransactionAbortedException, DbException {
    	if(i == null) return false;
        if(i.hasNext()) return true;

        if(currPageNumber+1 >= pagesCount) return false;

        HeapPageId tempHPI = new HeapPageId(id, currPageNumber+1);
        HeapPage tempHP = (HeapPage)Database.getBufferPool().getPage(tid, tempHPI, null);
        
        return tempHP.getNumEmptySlots() < tempHP.numSlots;
    }

    @Override
    public Tuple next() throws TransactionAbortedException, DbException {
    	if(i == null) throw new NoSuchElementException("iterator wasn't open");
        if(!i.hasNext()) goToNextPage();
    	return i.next();
    }

    @Override
    public void rewind() throws TransactionAbortedException, DbException {
    	if(i == null) throw new DbException("iterator wasn't open");
        close();
        open();
    }

    @Override
    public void close() {
    	currPageNumber = 0;
        hpid = null;
        hp = null;
        i = null;
    }
    
    private void goToNextPage() throws TransactionAbortedException, DbException {
    	currPageNumber++;
    	
        //this shouldn't happen
    	if(currPageNumber >= pagesCount) throw new NoSuchElementException(String.format("Page number %d is out of range", currPageNumber));
        
    	hpid = new HeapPageId(id, currPageNumber);
        hp = (HeapPage)Database.getBufferPool().getPage(tid, hpid, null);
        i = hp.iterator();
    }
}
