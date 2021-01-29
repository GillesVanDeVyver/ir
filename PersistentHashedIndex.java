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
    public static final long TABLESIZE = 611953L;
    
    public static final int DIRENTRYSIZE = 20;


    /** The dictionary hash table is stored in this file. */
    RandomAccessFile dictionaryFile;

    /** The data (the PostingsLists) are stored in this file. */
    RandomAccessFile dataFile;

    /** Pointer to the first free memory cell in the data file. */
    long free = 0L;
    

    /** The cache as a main-memory hash map. */
    HashMap<String,PostingsList> index = new HashMap<String,PostingsList>();


    // ===================================================================

    /**
     *   A helper class representing one entry in the dictionary hashtable.
     */ 
    public class Entry {
        long dataPtr;
        long checksum;
//        byte[] entryKey;
        int dataSize;
        
        public Entry(String word,long dataPtr,int dataSize) { // store checksum
        	this.dataPtr = dataPtr;
        	this.dataSize =dataSize;
        	this.checksum = checksum(word);
//            entryKey = new byte[DIRENTRYSIZE];
//            byte[] indexArr = longToByteArray(index);
//            byte[] checksumArr = longToByteArray(checksum);
//            byte[] dataSizeArr = intToByteArray(dataSize);
//        	for (int i =0; i <8;i++) {
//        		entryKey[i] = indexArr[i];
//        	}
//        	for (int i =0; i <8;i++) {
//        		entryKey[i+8] = checksumArr[i];
//        	}
//        	for (int i =0; i <4;i++) {
//        		entryKey[i+16] = dataSizeArr[i];
//        	}
//        	for (int i =0; i <20;i++) {
//        		System.out.println("entryKey[i]: " + entryKey[i]);
//        	}
//        	System.out.println("checksum: " + checksum);
//        	for (int i =0; i <8;i++) {
//        		System.out.println("checksumArr[i]: " + checksumArr[i]);
//        	}
//        	System.out.println("byteArrayToLong(checksumArr): " + byteArrayToLong(checksumArr));
        }
        
//        public String toString() { 
//        	String representation="";
//        	representation = representation+dataPtr+",";
//        	representation = representation+checksum+",";
//        	representation = representation+dataSize +",";
//    		return representation;
//        } 
        
      public ByteBuffer toByteByteBuffer() { 
    	  
    	ByteBuffer buffer = ByteBuffer.allocate(24);
    	buffer.putLong(0, dataPtr);
    	buffer.putLong(8, checksum);
    	buffer.putLong(16, dataSize);
		return buffer;
      } 
        

    }
    
//    public Entry stringToEntry(String representation, String word) {
//    	String delims = "[,]";
//    	String[] split = representation.split(delims);
//    	if (Long.parseLong(split[1])==checksum(word))
//    			return new Entry(word, Long.parseLong(split[0]),Integer.parseInt(split[2]));
//    	else
//    		return null;
//    }
    
//  public Entry arrToEntry(long[] arr, String word) {
//	  if (arr[1]==checksum(word))
//		  return new Entry(word, arr[0],arr[2]);
//	else
//		return null;
//}
    
	public long hash(String word) {
		byte[] wordBytes = word.getBytes();
		long hashValue = 0;
		int p = 31;
    	for (int i =0; i <wordBytes.length;i++) {
    		hashValue+= p*hashValue + wordBytes[i]; // String hashing using Polynomial rolling hash function  https://www.geeksforgeeks.org/string-hashing-using-polynomial-rolling-hash-function/
    	}
		return Math.abs(hashValue) % TABLESIZE;
	}
    
//	public Entry enrtyFromBytes(byte[] entryKey,String word) {
//        byte[] indexArr = new byte[8];
//        byte[] checksumArr = new byte[8];
//        byte[] dataSizeArr = new byte[4];
//    	for (int i =0; i <8;i++) {
//    		indexArr[i]= entryKey[i];
//    	}
//    	for (int i =0; i <8;i++) {
//    		checksumArr[i] = entryKey[i+8];
//    	}
//    	for (int i =0; i <4;i++) {
//    		dataSizeArr[i] = entryKey[i+16];
//    	}
//    	long index = byteArrayToLong(indexArr);
//    	long checksum = byteArrayToLong(checksumArr);
//    	int dataSize = byteArrayToInt(dataSizeArr);
//    	
//    	for (int i =0; i <20;i++) {
//    		System.out.println("read entryKey[i]: " + entryKey[i]);
//    	}
//    	for (int i =0; i <8;i++) {
//    		System.out.println("read checksumArr[i]: " + checksumArr[i]);
//    	}
//    	System.out.println("read byteArrayToLong(checksumArr): " + byteArrayToLong(checksumArr));
//    		System.out.println("stored checksum:" +checksum);
//    		System.out.println("calculated checksum:" + checksum(word));
//    		
//    	if (checksum == checksum(word)) {
////    		System.out.println("checksum matches");
//    		return new Entry(word,index, dataSize);
//    	}
//    	else {
//    		
//    		
//        	
//    		return null;
////    		return new Entry(word,index, dataSize);
//    	}		
//	}
	
	public long checksum(String word) {
		byte[] wordBytes = word.getBytes();
    	long checksum = 0;
		for (int i =0; i <wordBytes.length;i++) {
			checksum+= wordBytes[i];
		}
		return checksum;
	}
	

//	public int byteArrayToInt(byte[] array) {
//		int value = 0;
//		for (int i = 0; i < array.length; i++)
//		{
//			long toAdd = (long) array[i]<< (i * 8);
//			if (toAdd ==-1)
//				toAdd=255;
//		   value += toAdd;
//		}
//		return value;
//	}
//	
//	public int byteArrayToLong(byte[] array) {
//		int value = 0;
//		for (int i = 0; i < array.length; i++)
//		{
//			long toAdd = (long) array[i]<< (i * 8);
//			if (toAdd ==-1)
//				toAdd=255;
//		   value += toAdd;
//		}
//		return value;
//	}
//    
//
//    
//    public static  byte[] intToByteArray(int integer){
//    	byte[] bytes = new byte[4];
//    	for (int i = 0; i < 4; i++) {
//    	    bytes[i] = (byte)(integer >>> (i * 8));
//    	}
//    	return bytes;
//    }
//    
//    public static  byte[] longToByteArray(long l){
//    	byte[] bytes = new byte[8];
//    	for (int i = 0; i < 8; i++) {
//    	    bytes[i] = (byte)(l >>> (i * 8));
//    	}
//    	return bytes;
//    }


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
        try {
            dataFile.seek( ptr ); 
            byte[] data = dataString.getBytes();
            dataFile.write( data );
            return data.length;
        } catch ( IOException e ) {
            e.printStackTrace();
            return -1;
        }
    }


    /**
     *  Reads data from the data file
     */ 
    String readData( long ptr, int size ) {
        try {
//        	System.out.println("ptr data " + ptr);
            dataFile.seek( ptr );
            byte[] data = new byte[size];
            dataFile.readFully( data );
            return new String(data);
        } catch ( IOException e ) {
            e.printStackTrace();
            return null;
        }
    }
    
//    private void writeToPosition(String filename, int data, long position) 
//    		  throws IOException {
//    		    RandomAccessFile writer = new RandomAccessFile(filename, "rw");
//    		    writer.seek(position);
//    		    writer.writeInt(data);
//    		    writer.close();
//    		}


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
    	int collisions =0;
        try {
//            byte[] read = new byte[DIRENTRYSIZE];
//       	 System.out.println("data[0] writing to file: " + data[0]);
//            System.out.println("ptrFromIndex(index) writing : " + ptrFromIndex(index));

            try {
            	dictionaryFile.seek( ptrFromIndex(index) ); 
            	long dataPtr =dictionaryFile.readLong();
//            	System.out.println("readdataPtr" + dataPtr);
            if (dataPtr != 0 ) {
//            	System.out.println("collision");
            	collisions++;
            	long newIndex = index+1;
            	if (newIndex>=TABLESIZE)
            		newIndex =0;
//            	System.out.println(newIndex);
            	return collisions + writeEntry( entry,  newIndex );
            }
            else{
            	writeEntryAtIndex(entry,index);
            }
        	} catch ( EOFException e ) {
        		System.out.println("EOFEindex: " + index);
        		writeEntryAtIndex(entry,index);	
        	}
        } catch ( IOException e ) {
        	System.out.println("errorind: " + index);
            e.printStackTrace();
        }
        return collisions;
    }
    
    public void writeEntryAtIndex(Entry e, long index) throws IOException {
    	dictionaryFile.seek( ptrFromIndex(index) ); 
    	dictionaryFile.writeLong(e.dataPtr);
//        System.out.println("Storechecksum " + e.checksum);

    	dictionaryFile.writeLong(e.checksum);
    	dictionaryFile.writeInt(e.dataSize);
    }


    /**
     *  Reads an entry from the dictionary file.
     *
     *  @param index The index in the dictionary file where to start reading.
     *  @param word The word we want to read. Used for checking checksum
     */
    Entry readEntry( long index ,String word) {  
        System.out.println("ptrFromIndex(index) reading : " + ptrFromIndex(index));

        try {
        	dictionaryFile.seek( ptrFromIndex(index) );
            
            long readDataPtr = dictionaryFile.readLong();
            System.out.println("readDataPtr " + readDataPtr);

            long readChecksum = dictionaryFile.readLong();
            System.out.println("readChecksum " + readChecksum);
//            System.out.println("hash(word) " + hash(word));

            int readDataSize = dictionaryFile.readInt();
            System.out.println("readDataSize " + readDataSize);

            if (readChecksum!=checksum(word)) { //checksum didn't match
            	long newIndex = index+1;
            	if (newIndex>=TABLESIZE)
            		newIndex =0;
            	return readEntry(newIndex, word);
            }
            else {
            	return new Entry(word,readDataPtr,readDataSize);
            }
        } catch ( IOException e ) {
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
        FileOutputStream fout = new FileOutputStream( INDEXDIR + "/docInfo" );
        for (Map.Entry<Integer,String> entry : docNames.entrySet()) {
            Integer key = entry.getKey();
            String docInfoEntry = key + ";" + entry.getValue() + ";" + docLengths.get(key) + "\n";
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
    private void readDocInfo() throws IOException {
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
     */
    public void writeIndex() {
        int collisions = 0;
        try {
            // Write the 'docNames' and 'docLengths' hash maps to a file
            writeDocInfo();
            
            long dirIndex;
            long dataPtr = 0;
            // Write the dictionary and the postings list
            
            for (java.util.Map.Entry<String, PostingsList> e : index.entrySet()) {
            	String word = e.getKey();
                // write data
            	PostingsList pList = e.getValue();
            	
            	 if (word.equals("movie")) {
            		 System.out.println("storing representation : "+ pList.toString());
            	 }

            	
            	int dataLength = writeData( pList.toString(), dataPtr);
            	
//            	System.out.println("dataPtr " + dataPtr);
            	
            	// write dictionary
                
                Entry dirEntry = new Entry(word,dataPtr, dataLength);

                dirIndex = hash(word);
                if (word.equals("movie")) {
                	System.out.println("storing word " + word);
                	System.out.println("storing dataPtr " + dataPtr);
                	System.out.println("storing dirEntry.dataSize " + dirEntry.dataSize);
                	System.out.println("storing dirEntry.checksum " + dirEntry.checksum);
                	System.out.println("storing dirEntry.dataPtr " + dirEntry.dataPtr);
                	System.out.println("storing dirIndex " + dirIndex);
                }

                	collisions += writeEntry(dirEntry, dirIndex);

                	dataPtr+=dataLength;
//                System.out.println("hash(word) " + hash(word));
//                collisions += writeEntry(dirEntry, dirIndex);
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
		 long dirIndex = hash(token);
		 Entry dirEntry = readEntry( dirIndex, token);
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
