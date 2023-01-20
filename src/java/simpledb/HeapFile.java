package simpledb;

import java.io.*;
import java.util.*;
/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

	//CHANGES
	File f;
	TupleDesc td;
	int id;
	
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {//CHANGES
        this.f = f;
        this.td = td;
        this.id = f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {//CHANGES
        return f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {//CHANGES
        return id;
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {//CHANGES
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {//CHANGES
    	int pageSize = BufferPool.getPageSize();
    	int pageNumber  = pid.getPageNumber();
    	
        try 
        {
        	HeapPageId hpid = new HeapPageId(getId(), pageNumber);
        	
        	RandomAccessFile raf = new RandomAccessFile(f, "r");
        	
        	long offset = pageSize * pageNumber;
        	byte[] buffer = new byte[pageSize];
        	
        	raf.seek(offset);
        	raf.read(buffer,0,pageSize);//buffer, byteOffset, byteCount
        	raf.close();
        	
        	return new HeapPage(hpid, buffer);
        }
        catch(IOException e) 
        {
        	e.printStackTrace();
        }
    	
    	return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException { //CHANGES
        try (RandomAccessFile file = new RandomAccessFile(f, "rw")) {
            file.seek(page.getId().getPageNumber() * BufferPool.getPageSize());
            file.write(page.getPageData());
        } catch (Exception e) {
            throw new IOException("write failed");
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {//CHANGES
        return (int)(f.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException { // CHANGES
        ArrayList<Page> writtenPages = new ArrayList<>();

        for(int i = 0; i < this.numPages(); ++i){
            HeapPageId hpid = new HeapPageId(getId(), i);
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, hpid, Permissions.READ_WRITE);

            if (page.getNumEmptySlots() != 0) {
                page.insertTuple(t);
                page.markDirty(true, tid);
                writtenPages.add(page);
                return writtenPages;
            }
        }

        HeapPageId hpid = new HeapPageId(getId(), this.numPages());
        HeapPage blank = (HeapPage) Database.getBufferPool().getPage(tid, hpid, Permissions.READ_WRITE);
        writePage(blank);

        HeapPage newPage = (HeapPage) Database.getBufferPool().getPage(tid, hpid, Permissions.READ_WRITE);
        newPage.insertTuple(t);
        newPage.markDirty(true, tid);
        writtenPages.add(newPage);

        return writtenPages;
    }


    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException { //CHANGES
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, t.getRecordId().getPageId(), Permissions.READ_WRITE);
        page.deleteTuple(t);
        return new ArrayList<>(Collections.singletonList(page));
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {//CHANGES
        return new HeapFileIterator(numPages(),id, tid);
    }

}

