package ir;

import java.io.BufferedReader;
import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.TimeUnit;


import ir.PersistentHashedIndex.Entry;

public class PersistentScalableHashedIndex extends PersistentHashedIndex implements Index  {
	
	
//	public static final long TOKENTHRESH = 500000; //med daviswiki
    public static final long TOKENTHRESH = 2500000; //full daviswiki
//	public static final long TOKENTHRESH = 25000L;
//	public static final long TOKENTHRESH = 2;
//	public static final long TOKENTHRESH = 10000000; //guardian

	
    public static final String INTERMEDIATE_DICTIONARY_FNAME = "tempDictionary";

    public static final String INTERMEDIATE_DATA_FNAME = "tempData";	
    
    
    private LinkedList<Merger> threadQueue = new LinkedList<Merger>();

    
	long tokenCounter = 0L;
	
	int intermediateCounter = 0;
	
	
    private LinkedList<RandomAccessFile> mergeQueue = new LinkedList<RandomAccessFile>();
	
    public PersistentScalableHashedIndex(boolean is_indexing) {
 
        try {
        	if (is_indexing) {
	            dictionaryFile = generateDictFileName();
	            dataFile = generateDataFileName();
        	}
        	else {
        		dictionaryFile = new RandomAccessFile( INDEXDIR + "/" + DICTIONARY_FNAME, "rw" );
                dataFile = new RandomAccessFile( INDEXDIR + "/" + DATA_FNAME, "rw" );
        	}
            readDocInfo();
            
        } catch ( FileNotFoundException e ) {
        	
        } catch ( IOException e ) {
            e.printStackTrace();
        }
    }
    
    public PersistentScalableHashedIndex(RandomAccessFile dictFile, RandomAccessFile dFile) {

        dictionaryFile = dictFile;
        dataFile = dFile;

//        try {
//            readDocInfo();
//        } catch ( FileNotFoundException e ) {
//        	
//        } catch ( IOException e ) {
//            e.printStackTrace();
//        }
    }
    


    public void insert( String token, int docID, int offset ) {
    	
    	super.insert(token, docID, offset);
    	tokenCounter++;
    	
    	if (tokenCounter==TOKENTHRESH) {
			tokenCounter = 0;
	        System.err.println( index.keySet().size() + " unique words" );
	        System.err.print( "Writing index to disk..." );
			writeIndex();
			docNames.clear();
			docLengths.clear();
			index = new HashMap<String,PostingsList>();
			dataWritePtr = 0;
			synchronized ( mergeQueue ) {
				mergeQueue.add(dictionaryFile);
				mergeQueue.add(dataFile);
			}
			if (mergeQueue.size()>=4) {
				merge(false);
			}
			
			intermediateCounter++;
			dictionaryFile = generateDictFileName();
			dataFile = generateDataFileName();
    	}
    }
    

    



	private RandomAccessFile generateDictFileName() {
		RandomAccessFile result = null;
		try {
			System.out.println("intermediateCounter" + intermediateCounter);
			String tempNameDict = INDEXDIR + "/" + INTERMEDIATE_DICTIONARY_FNAME + intermediateCounter;
			result = new RandomAccessFile( tempNameDict, "rw" );
//			System.out.println("tempNameDict " + tempNameDict + result);

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		return result;
	}
	
	private RandomAccessFile generateDataFileName() {
		RandomAccessFile result = null;
		try {
			String tempNameData = INDEXDIR + "/" + INTERMEDIATE_DATA_FNAME + intermediateCounter;
			result = new RandomAccessFile( tempNameData, "rw" );
//			System.out.println("tempNameData " + tempNameData + result);

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		return result;
	}

	private void merge(boolean cleanup) {
		RandomAccessFile firstDict;
		RandomAccessFile secondData;
		RandomAccessFile firstData;
		RandomAccessFile secondDict;
		synchronized ( mergeQueue ) {
			firstDict = mergeQueue.removeFirst();
			firstData = mergeQueue.removeFirst();
			secondDict = mergeQueue.removeFirst();
			secondData = mergeQueue.removeFirst();
		}
			intermediateCounter++;
			RandomAccessFile resultDict= null;
			RandomAccessFile resultData = null;
			if (cleanup){
				try {
					resultDict = new RandomAccessFile( INDEXDIR + "/" + DICTIONARY_FNAME, "rw" );
					resultData = new RandomAccessFile( INDEXDIR + "/" + DATA_FNAME, "rw" );
				} catch (FileNotFoundException e) {
					e.printStackTrace();
				}
				
			}
			else {
				resultDict = generateDictFileName();
				resultData = generateDataFileName();		
			}
	
	
			
			Merger merger = new Merger(firstDict,firstData,secondDict,secondData,
					resultDict, resultData,mergeQueue,threadQueue );
			synchronized ( threadQueue ) {
				threadQueue.add(merger);
			}
			System.out.println("threadQueue" + threadQueue);
			merger.start();
			
//			try {
//				merger.join();
//			} catch (InterruptedException e) {
//				e.printStackTrace();
//			}


	}

    public void cleanup() {
        System.err.println( index.keySet().size() + " unique words" );
        System.err.print( "Writing index to disk..." );
		writeIndex();
		synchronized ( mergeQueue ) {
			mergeQueue.add(dictionaryFile);
			mergeQueue.add(dataFile);
		}
		waitForThreads();
		// while threadqueue.size>=1 threadqueu.removefirst join if mergeque>4 new thread
//		while(mergeQueue.size()>4) {
//			merge(false);
//		}
//		if(mergeQueue.size()==4) {
//			merge(true);
//		}
		
		dictionaryFile = mergeQueue.removeFirst();
		dataFile = mergeQueue.removeFirst();
		
        try {
            readDocInfo();
            deleteDocInfoFile();
            writeDocInfo();
        } catch ( FileNotFoundException e ) {
        	
        } catch ( IOException e ) {
            e.printStackTrace();
        }
        

        System.err.println( "done!" );
    }
    
    private void waitForThreads() {
    	
    	while (threadQueue.size()>=1) {
    		Thread firstThread;
    		synchronized ( threadQueue ) {
	    		firstThread = threadQueue.removeFirst();
    		}
    		try {
				firstThread.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
    		System.out.println("mergeQueue.size()"+ mergeQueue.size());
    		synchronized ( mergeQueue ) {
	    		while(mergeQueue.size()>4) {
	    			merge(false);
	    		}
    		}
    	}
    	if(mergeQueue.size()==4) {
			merge(true);
		}
    	Merger lastThread;
		synchronized ( threadQueue ) {
    		lastThread = threadQueue.removeFirst();
		}
    	try {
			lastThread.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
    		
		
	}

	void deleteDocInfoFile() {
    	File file = new File( INDEXDIR + "/docInfo" );
    	file.delete();
    }
    
    public Entry readEntryAndDel (long index ,long checksum, RandomAccessFile file) {  

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
            	file.seek( ptrFromIndex(index) );
                file.writeLong(0L);
                file.writeLong(0L);
                file.writeInt(0);
            	return new Entry(checksum,readDataPtr,readDataSize, index);
            }
        } 
        catch ( EOFException e ) {
        	System.out.println("EOFException");
            return null;
        }catch ( IOException e ) {
            e.printStackTrace();
            return null;
        }
    }
    


}
    















