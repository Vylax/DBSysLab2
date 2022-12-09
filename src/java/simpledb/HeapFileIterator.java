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

    public void open() throws TransactionAbortedException, DbException {
    	currPageNumber = 0;
        hpid = new HeapPageId(id, currPageNumber);
        hp = (HeapPage)Database.getBufferPool().getPage(tid, hpid, null);
        i = hp.iterator();
    }

    public boolean hasNext() {
        return i.hasNext();
    }

    public Tuple next() throws TransactionAbortedException, DbException {
        if(!i.hasNext()) goToNextPage();
    	return i.next();
    }

    public void rewind() throws TransactionAbortedException, DbException {
        close();
        open();
    }

    public void close() {
    	currPageNumber = 0;
        hpid = null;
        hp = null;
        i = null;
    }
    
    private void goToNextPage() throws TransactionAbortedException, DbException {
    	currPageNumber++;
    	
    	if(currPageNumber >= pagesCount) throw new NoSuchElementException(String.format("Page number %d is out of range", currPageNumber));
        
    	hpid = new HeapPageId(id, currPageNumber);
        hp = (HeapPage)Database.getBufferPool().getPage(tid, hpid, null);
        i = hp.iterator();
    }
}
