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
//    public static final long TABLESIZE = 3499999L; //Guardian
    public static final long TABLESIZE = 611953L; //DavisWiki
    
    public static final int DIRENTRYSIZE = 20;
    
    public static final int HASHPRIME = 137;
    
    public static final int CHECKSUMPRIME1 = 97;
    
    public static final int CHECKSUMPRIME2 = 999979;
    
    
    /** The dictionary hash table is stored in this file. */
    RandomAccessFile dictionaryFile;

    /** The data (the PostingsLists) are stored in this file. */
    RandomAccessFile dataFile;

    /** Pointer to the first free memory cell in the data file. */
    long free = 0L;
    
    /** Pointer to the where data is written in the data file. */
    long dataWritePtr = 0L;    

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
        
        /**
         *  Constructor. calculates the checksum from the given word
         *  @param word Token corresponding to this entry
         *  @param dataPtr Where the data is stored in the dataFile
         *  @param dataSize The size of the data in the dataFile
         */
        public Entry(String word,long dataPtr,int dataSize) { // store checksum
        	this.dataPtr = dataPtr;
        	this.dataSize =dataSize;
        	this.checksum = checksum(word);
        }
        
        /**
         *  Constructor. calculates the checksum from the given word
         *  @param checksum Checksum used to check token retrieval
         *  @param dataPtr Where the data is stored in the dataFile
         *  @param dataSize The size of the data in the dataFile
         */
        public Entry(long checksum,long dataPtr,int dataSize) { // store checksum
        	this.dataPtr = dataPtr;
        	this.dataSize =dataSize;
        	this.checksum = checksum;
        }
    }
    
    /**
     *  Creates a hash value for the given token using a polynomial rolling hash function
     *
     *  @param word The token to calculate the hash value for
     *  @return The hash value
     */
	public long hash(String word) {
		byte[] wordBytes = word.getBytes();
		long hashValue = 0;
;
    	for (int i =0; i <wordBytes.length;i++) {
    		hashValue+= HASHPRIME*hashValue + wordBytes[i]; // String hashing using a polynomial rolling hash function  https://www.geeksforgeeks.org/string-hashing-using-polynomial-rolling-hash-function/
    	}
		return Math.abs(hashValue) % TABLESIZE;
	}
    
    /**
     *  Creates a checksum value for the given token using a polynomial rolling hash function
     *
     *  @param word The token to calculate the checksum value for
     *  
     *  @return The checksum value
     */
	public long checksum(String word) {
		byte[] wordBytes = word.getBytes();
		long checksum = 0;
;
    	for (int i =0; i <wordBytes.length;i++) {
    		checksum+= CHECKSUMPRIME1*checksum + wordBytes[i]; // String hashing using Polynomial rolling hash function  https://www.geeksforgeeks.org/string-hashing-using-polynomial-rolling-hash-function/
    	}
		return Math.abs(checksum) % CHECKSUMPRIME2;
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
    
    /**
     *  Writes data to the data file at a specified place in a specified file.
     *  
     *  @param file The file the data is written to.
     *
     *  @return The number of bytes written.
     */ 
    static int writeData( String dataString, long ptr, RandomAccessFile file) {
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
    static String readData( long ptr, int size, RandomAccessFile file) {
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
     *  Also handles collisions by only writing to the first free spot.
     *
     *  @param entry The key of this entry is assumed to have a fixed length
     *  @param index   The index in the dictionary file to store the entry
     */
    int writeEntry( Entry entry, long index ) {
    	return writeEntry(  entry,  index,  dictionaryFile );
    }
    
    /*
     *  Writes an entry to the dictionary hash table file in a specified file.
     *  Also handles collisions by only writing to the first free spot.
     *
     *  @param file The file the data is read from.
     *  @param entry The key of this entry is assumed to have a fixed length
     *  @param index   The index in the dictionary file to store the entry
     */
    int writeEntry( Entry entry, long index, RandomAccessFile file ) {
        try {
            try {
            	file.seek( ptrFromIndex(index) ); 
                long readDataPtr = dictionaryFile.readLong();
                long readChecksum = dictionaryFile.readLong();
                int readDataSize = dictionaryFile.readInt();
            if (readDataSize != 0 ) {
            	long newIndex = index+1;
            	return 1 + writeEntry( entry,  newIndex, file );
            }
            else{
            	writeEntryAtIndex(entry,index, file);
            }
        	} catch ( EOFException e ) {
        		writeEntryAtIndex(entry,index, file);	
        	}
        } catch ( IOException e ) {
        	System.err.println("errorind: " + index);
            e.printStackTrace();
        }
        return 0;
    }
    
    /*
     *  Writes an entry to the dictionary hash table file at specified index. 
     *
     *  @param entry The key of this entry is assumed to have a fixed length
     *  @param index   The index in the dictionary file to store the entry
     */
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
     *  Reads an entry from the dictionary file and checks if the checksum matches.
     *
     *  @param index The index in the dictionary file where to start reading.
     *  @param checksum The checksum to check
     *  
     *  @return The found entry
     */
    Entry readEntryAndCheck( long index ,long checksum) {  
    	return readEntryAndCheck( index ,checksum, dictionaryFile);
    }
    
    Entry readEntryAndCheck( long index ,long checksum, RandomAccessFile file) {  

        try {
        	file.seek( ptrFromIndex(index) );
            long readDataPtr = file.readLong();
            long readChecksum = file.readLong();
            int readDataSize = file.readInt();
            if (readDataSize == 0) {
            	return null;
            }
            if (readChecksum!=checksum) { //checksum didn't match
            	long newIndex = index+1;
            	return readEntryAndCheck(newIndex, checksum);
            }
            else {
            	return new Entry(checksum,readDataPtr,readDataSize);
            }
        } 
        catch ( EOFException e ) {
            return null;
        }catch ( IOException e ) {
            e.printStackTrace();
            return null;
        }
    }
    
    /**
     *  Converts a directory file index to a byte position in the file
     *  
     *  @param index The index to convert
     *
     *  @return The pointer to the right byte position in the directory file
     */ 
    public static long ptrFromIndex(long index) {
    	return index*DIRENTRYSIZE;
    }


    // ==================================================================

    /**
     *  Writes the document names and document lengths to file.
     *
     * @throws IOException  { exception_description }
     */
    public void writeDocInfo() throws IOException {
        FileOutputStream fout = new FileOutputStream( INDEXDIR + "/docInfo" , true);
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
    public int writeIndex()  {
        int collisions = 0;
    	
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
        return collisions;
    }
    
    



    // ==================================================================


    /**
     *  Returns the postings for a specific term, or null
     *  if the term is not in the index.
     */
    public PostingsList getPostings( String token ) {
		 long dirIndex = hash(token);
		 Entry dirEntry = readEntryAndCheck( dirIndex, checksum(token));
		 String dataString = readData( dirEntry.dataPtr, dirEntry.dataSize);
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
		writeIndex();
        System.err.println( "done!" );
    }
}
