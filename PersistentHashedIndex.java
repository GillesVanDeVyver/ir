/*  
 *   This file is part of the computer assignment for the
 *   Information Retrieval course at KTH.
 * 
 *   Johan Boye, KTH, 2018
 */  

package ir;

import java.io.*;
import java.util.*;
import com.google.common.primitives.Longs;

import ir.PersistentHashedIndex.Entry;

import java.nio.ByteBuffer;
import java.nio.charset.*;


/*
 *   Implements an inverted index as a hashtable on disk.
 *   
 *   Both the words (the dictionary) and the data (the postings list) are
 *   stored in RandomAccessFiles that permit fast (almost constant-time)
 *   disk seeks. 
 *
 *   When words are read and indexed, they are first put in an ordinary,
 *   main-memory HashMap. When all words are read, the index is committed
 *   to disk.
 */
public class PersistentHashedIndex implements Index {

    /** The directory where the persistent index files are stored. */
    public static final String INDEXDIR = "./index";

    /** The dictionary file name */
    public static final String DICTIONARY_FNAME = "dictionary";

    /** The dictionary file name */
    public static final String DATA_FNAME = "data";

    /** The terms file name */
    public static final String TERMS_FNAME = "terms";

    /** The doc info file name */
    public static final String DOCINFO_FNAME = "docInfo";

    /** The dictionary hash table on disk can fit this many entries. */
//    public static final long TABLESIZE = 3499999L;
    public static final long TABLESIZE = 17L;
   
    public static final int DIRENTRYSIZE = 20;


    /** The dictionary hash table is stored in this file. */
    RandomAccessFile dictionaryFile;

    /** The data (the PostingsLists) are stored in this file. */
    RandomAccessFile dataFile;

    /** Pointer to the first free memory cell in the data file. */
    long free = 0L;
    
    long dataWritePtr = 0L;
    
    int collisions = 0;
    

    /** The cache as a main-memory hash map. */
    HashMap<String,PostingsList> index = new HashMap<String,PostingsList>();


    // ===================================================================

    /**
     *   A helper class representing one entry in the dictionary hashtable.
     */ 
    public class Entry {
        long dataPtr;
        long checksum;
        int dataSize;
        long index;
        
        public Entry(String word,long dataPtr,int dataSize) { // store checksum
        	this.dataPtr = dataPtr;
        	this.dataSize =dataSize;
        	this.checksum = checksum(word);
        }
        
        public Entry(long checksum,long dataPtr,int dataSize) { // store checksum
        	this.dataPtr = dataPtr;
        	this.dataSize =dataSize;
        	this.checksum = checksum;
        }
        
        public Entry(long checksum,long dataPtr,int dataSize, long index) { // store checksum
        	this.dataPtr = dataPtr;
        	this.dataSize =dataSize;
        	this.checksum = checksum;
        	this.index = index;
        }
        
      public ByteBuffer toByteByteBuffer() { 
    	  
    	ByteBuffer buffer = ByteBuffer.allocate(24);
    	buffer.putLong(0, dataPtr);
    	buffer.putLong(8, checksum);
    	buffer.putLong(16, dataSize);
		return buffer;
      } 
        

    }
    
    
	public long hash(String word) {
		byte[] wordBytes = word.getBytes();
		long hashValue = 0;
		int p = 31;
    	for (int i =0; i <wordBytes.length;i++) {
    		hashValue+= p*hashValue + wordBytes[i]; // String hashing using Polynomial rolling hash function  https://www.geeksforgeeks.org/string-hashing-using-polynomial-rolling-hash-function/
    	}
		return Math.abs(hashValue) % TABLESIZE;
	}
    
	
	public long checksum(String word) {
		byte[] wordBytes = word.getBytes();
    	long checksum = 0;
		for (int i =0; i <wordBytes.length;i++) {
			checksum+= wordBytes[i];
		}
		return checksum;
	}
	


    // ==================================================================

    
    /**
     *  Constructor. Opens the dictionary file and the data file.
     *  If these files don't exist, they will be created. 
     */
    public PersistentHashedIndex() {
        try {
            dictionaryFile = new RandomAccessFile( INDEXDIR + "/" + DICTIONARY_FNAME, "rw" );
            dataFile = new RandomAccessFile( INDEXDIR + "/" + DATA_FNAME, "rw" );
        } catch ( IOException e ) {
            e.printStackTrace();
        }

        try {
            readDocInfo();
        } catch ( FileNotFoundException e ) {
        	
        } catch ( IOException e ) {
            e.printStackTrace();
        }
    }

    /**
     *  Writes data to the data file at a specified place.
     *
     *  @return The number of bytes written.
     */ 
    int writeData( String dataString, long ptr ) {
    	return writeData(dataString, ptr, dataFile);
    }
    
    int writeData( String dataString, long ptr, RandomAccessFile file) {
        try {
        	file.seek( ptr ); 
            byte[] data = dataString.getBytes();
            file.write( data );
            return data.length;
        } catch ( IOException e ) {
            e.printStackTrace();
            return -1;
        }
    }


    /**
     *  Reads data from the given file
     */ 
    String readData( long ptr, int size, RandomAccessFile file) {
    	System.out.println("read");
        try {
        	file.seek( ptr );
            byte[] data = new byte[size];
            file.readFully( data );
            return new String(data);
        } catch ( IOException e ) {
            e.printStackTrace();
            return null;
        }
    }
    
    /**
     *  Reads data from the data file
     */ 
    String readData( long ptr, int size ) {
        try {
            dataFile.seek( ptr );
            byte[] data = new byte[size];
            dataFile.readFully( data );
            return new String(data);
        } catch ( IOException e ) {
            e.printStackTrace();
            return null;
        }
    }


    // ==================================================================
    //
    //  Reading and writing to the dictionary file.

    /*
     *  Writes an entry to the dictionary hash table file. 
     *
     *  @param entry The key of this entry is assumed to have a fixed length
     *  @param index   The index in the dictionary file to store the entry
     */
    int writeEntry( Entry entry, long index ) {
    	return writeEntry(  entry,  index,  dictionaryFile );
    }
    
    int writeEntry( Entry entry, long index, RandomAccessFile file ) {
        try {


            try {
            	file.seek( ptrFromIndex(index) ); 
            	long dataPtr =file.readLong();
            if (dataPtr != 0 ) {
            	collisions++;
            	long newIndex = index+1;
            	if (newIndex>=TABLESIZE)
            		newIndex =0;
            	return collisions + writeEntry( entry,  newIndex, file );
            }
            else{
            	writeEntryAtIndex(entry,index, file);
            }
        	} catch ( EOFException e ) {
//        		System.out.println("EOFEindex: " + index);
        		writeEntryAtIndex(entry,index, file);	
        	}
        } catch ( IOException e ) {
        	System.out.println("errorind: " + index);
            e.printStackTrace();
        }
        return collisions;
    }
    
    public void writeEntryAtIndex(Entry e, long index) throws IOException {
    	writeEntryAtIndex(e,  index, dictionaryFile);
    }
    
    public void writeEntryAtIndex(Entry e, long index, RandomAccessFile file) throws IOException {
    	file.seek( ptrFromIndex(index) ); 
    	file.writeLong(e.dataPtr);
    	file.writeLong(e.checksum);
    	file.writeInt(e.dataSize);
    }

    
    /**
     *  Reads an entry from the dictionary file.
     *
     *  @param index The index in the dictionary file where to start reading.
     * @throws IOException 
     */
    Entry readEntry( long index) throws IOException {  
		dictionaryFile.seek( ptrFromIndex(index) );
        long readDataPtr = dictionaryFile.readLong();
        long readChecksum = dictionaryFile.readLong();
        int readDataSize = dictionaryFile.readInt();
        return new Entry(readChecksum,readDataPtr,readDataSize);
    }
    
    Entry readEntry( long index, RandomAccessFile file) throws IOException {  
    	file.seek( ptrFromIndex(index) );
        long readDataPtr = file.readLong();
        long readChecksum = file.readLong();
        int readDataSize = file.readInt();
        return new Entry(readChecksum,readDataPtr,readDataSize);
    }

    /**
     *  Reads an entry from the dictionary file.
     *
     *  @param index The index in the dictionary file where to start reading.
     *  @param word The word we want to read. Used for checking checksum
     */
//    Entry readEntryAndCheck( long index ,String word) {  
//
//        try {
//        	dictionaryFile.seek( ptrFromIndex(index) );
//            long readDataPtr = dictionaryFile.readLong();
//            long readChecksum = dictionaryFile.readLong();
//            int readDataSize = dictionaryFile.readInt();
//            if (readChecksum!=checksum(word)) { //checksum didn't match
//            	long newIndex = index+1;
//            	if (newIndex>=TABLESIZE)
//            		newIndex =0;
//            	return readEntryAndCheck(newIndex, word);
//            }
//            else {
//            	return new Entry(word,readDataPtr,readDataSize);
//            }
//        } catch ( IOException e ) {
//            e.printStackTrace();
//            return null;
//        }
//    }
    
    Entry readEntryAndCheck( long index ,long checksum) {  
    	return readEntryAndCheck( index ,checksum, dictionaryFile);
    }
    
    Entry readEntryAndCheck( long index ,long checksum, RandomAccessFile file) {  
//    	System.out.println("index " + index);
//    	System.out.println("file " + file);
        try {
//        	System.out.println("ok1 ");
        	file.seek( ptrFromIndex(index) );
//        	System.out.println("ok2 ");
            long readDataPtr = file.readLong();
//            System.out.println("ok3 ");


            	
            long readChecksum = file.readLong();
//            System.out.println("readChecksum " + readChecksum);
            int readDataSize = file.readInt();
            if (readDataSize == 0) {
//            	System.out.println("readDataPtr == 0 " );
            	return null;
            }
            if (readChecksum!=checksum) { //checksum didn't match
            	long newIndex = index+1;
            	if (newIndex>=TABLESIZE)
            		newIndex =0;
            	return readEntryAndCheck(newIndex, checksum);
            }
            else {
            	return new Entry(checksum,readDataPtr,readDataSize, index);
            }
        } 
        catch ( EOFException e ) {
            return null;
        }catch ( IOException e ) {
            e.printStackTrace();
            return null;
        }
    }
    
    
    public long ptrFromIndex(long index) {
    	return index*DIRENTRYSIZE;
    }


    // ==================================================================

    /**
     *  Writes the document names and document lengths to file.
     *
     * @throws IOException  { exception_description }
     */
    private void writeDocInfo() throws IOException {
        FileOutputStream fout = new FileOutputStream( INDEXDIR + "/docInfo", true );
        for (Map.Entry<Integer,String> entry : docNames.entrySet()) {
            Integer key = entry.getKey();
            String docInfoEntry = key + ";" + entry.getValue()+ ";" + docLengths.get(key) + "\n";
            fout.write(docInfoEntry.getBytes());
        }
        fout.close();
    }


    /**
     *  Reads the document names and document lengths from file, and
     *  put them in the appropriate data structures.
     *
     * @throws     IOException  { exception_description }
     */
    protected void readDocInfo() throws IOException {
        File file = new File( INDEXDIR + "/docInfo" );
        FileReader freader = new FileReader(file);
        try (BufferedReader br = new BufferedReader(freader)) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] data = line.split(";");
                docNames.put(new Integer(data[0]), data[1]);
                docLengths.put(new Integer(data[0]), new Integer(data[2]));
            }
        }
        freader.close();
    }


    /**
     *  Write the index to files.
     * @throws IOException 
     */
    public void writeIndex() throws IOException {
//        int collisions = 0;
    	
//    	try {
//    	dictionaryFile.seek( ptrFromIndex(TABLESIZE+1) ); 
//    	dictionaryFile.writeInt(0);
//    	} catch ( EOFException e ) {
//    		dictionaryFile.seek( ptrFromIndex(TABLESIZE) ); 
//        	dictionaryFile.writeLong(0);	
//    } catch ( IOException e ) {
//    	System.out.println("error writing end file" );
//        e.printStackTrace();
//    }
    	dataWritePtr = 0;
        try {

            // Write the 'docNames' and 'docLengths' hash maps to a file
            this.writeDocInfo();
            
            // Write the dictionary and the postings list
            
            for (java.util.Map.Entry<String, PostingsList> e : index.entrySet()) {
            	String word = e.getKey();
            	
                // write data
            	PostingsList pList = e.getValue();
            	int dataLength = writeData( pList.toString(), dataWritePtr);
            	
            	// write dictionary
                Entry dirEntry = new Entry(word,dataWritePtr, dataLength);
            	collisions += writeEntry(dirEntry, hash(word));
            	dataWritePtr+=dataLength;
            }
            

        } catch ( IOException e ) {
            e.printStackTrace();
        }
        System.err.println( collisions + " collisions." );
    }
    
    



    // ==================================================================


    /**
     *  Returns the postings for a specific term, or null
     *  if the term is not in the index.
     */
    public PostingsList getPostings( String token ) {
    	System.out.println("token " + token);
		 long dirIndex = hash(token);
		 System.out.println("dirIndex " + dirIndex);
		 System.out.println("checksum(token) " + checksum(token));
		 Entry dirEntry = readEntryAndCheck( dirIndex, checksum(token));
		 String dataString = readData( dirEntry.dataPtr, dirEntry.dataSize);
		 System.out.println("dataString " + dataString);

		 System.out.println("getPostingsResult " + PostingsList.stringToObj(dataString));
		 return PostingsList.stringToObj(dataString);
    }
   

    /**
     *  Inserts this token in the main-memory hashtable.
     */
    public void insert( String token, int docID, int offset ) {
		PostingsList list = index.get(token);
		if (list==null) {
	    	list = new PostingsList();
		}
		list.add(docID, offset);
		index.put(token, list);
    }


    /**
     *  Write index to file after indexing is done.
     */
    public void cleanup() {
        System.err.println( index.keySet().size() + " unique words" );
        System.err.print( "Writing index to disk..." );
        try {
			writeIndex();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        System.err.println( "done!" );
    }
}
