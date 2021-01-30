package ir;

import java.io.BufferedReader;
import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import ir.PersistentHashedIndex.Entry;

public class PersistentScalableHashedIndex extends PersistentHashedIndex implements Index  {
	
    public static final int TOKENTHRESHOLD = 1;

    public static final String INTERMEDIATE_DICTIONARY_FNAME = "tempDictionary";

    /** The dictionary file name */
    public static final String INTERMEDIATE_DATA_FNAME = "tempData";

    /** The terms file name */
    public static final String INTERMEDIATE_TERMS_FNAME = "tempTerms";

    /** The doc info file name */
    public static final String INTERMEDIATE_DOCINFO_FNAME = "tempDocInfo";
    
    int nbTokensCurrent = 0;
    
    int intermediateCounter = 0;
    

    
    private LinkedList<RandomAccessFile> intermediateList = new LinkedList<RandomAccessFile>();

    
    public PersistentScalableHashedIndex() {
        try {
        	dictionaryFile = new RandomAccessFile( INDEXDIR + "/" + DICTIONARY_FNAME, "rw" );
            dataFile = new RandomAccessFile( INDEXDIR + "/" + DATA_FNAME, "rw" );
        } catch ( FileNotFoundException e ) {
        	try {
            dictionaryFile = new RandomAccessFile( INDEXDIR + "/" + INTERMEDIATE_DICTIONARY_FNAME +0, "rw" );
            dataFile = new RandomAccessFile( INDEXDIR + "/" + INTERMEDIATE_DATA_FNAME + 0, "rw" );
        }catch ( IOException e1 ) {
            e.printStackTrace();
        } }
 

        try {
            readDocInfo();
        } catch ( FileNotFoundException e ) {
        	
        } catch ( IOException e ) {
            e.printStackTrace();
        }
    }
    


    public void insert( String token, int docID, int offset ) {
    	System.out.println("token " + token);
		PostingsList list = index.get(token);
		if (list==null) {
	    	list = new PostingsList();
		}
		list.add(docID, offset);
		index.put(token, list);
    	nbTokensCurrent++;
    	if (nbTokensCurrent>TOKENTHRESHOLD) {
    		    		
    		nbTokensCurrent = 0;
    		// store index on disk
    		try {
    			dataWritePtr = 0;
				writeIndex();
//				try {
//					try {
//						Entry e = readEntry(10, dictionaryFile);	
//					}   
//					catch (EOFException e) {
//						System.out.println("EOFE at 10");
//					}
//					try {
//						Entry e = readEntry(11, dictionaryFile);	
//					}   
//					catch (EOFException e) {
//						System.out.println("EOFE at 11");
//					}
//					try {
//						Entry e = readEntry(12, dictionaryFile);	
//					}   
//					catch (EOFException e) {
//						System.out.println("EOFE at 12");
//					}
//					try {
//						Entry e = readEntry(13, dictionaryFile);	
//					}   
//					catch (EOFException e) {
//						System.out.println("EOFE at 13");
//					}
//					try {
//						Entry e = readEntry(14, dictionaryFile);	
//					}   
//					catch (EOFException e) {
//						System.out.println("EOFE at 14");
//					}
//					TimeUnit.SECONDS.sleep(30);
//				} catch (InterruptedException e) {
//					// TODO Auto-generated catch block
//					e.printStackTrace();
//				}

			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
    		intermediateList.add(dictionaryFile);
    		intermediateList.add(dataFile);
    		
    		// clear index in main memory
			index = new HashMap<String,PostingsList>();
    		
    		// clear docNames (main memory)
			docNames.clear();
			
    		// clear docLengths (main memory)
			docLengths.clear();
    		
    		// reset free pointer
    		dataWritePtr = 0;
    		
    		//recreate dictionaryFile and dataFile
    		
    		
    		
			try {
				// if >= 2 data files => merge
				merge(false); // todo seperate thread
				intermediateCounter++;
				System.out.println("intermediateCounter" + intermediateCounter);
				String tempNameDict = INDEXDIR + "/" + INTERMEDIATE_DICTIONARY_FNAME + intermediateCounter;
				dictionaryFile = new RandomAccessFile( tempNameDict, "rw" );
				System.out.println("tempName " + tempNameDict);
				String tempNameData = INDEXDIR + "/" + INTERMEDIATE_DATA_FNAME + intermediateCounter;
				dataFile = new RandomAccessFile( tempNameData, "rw" );
				System.out.println("tempName " + tempNameData);
			} catch ( IOException e ) {
	            e.printStackTrace();
	        }
    	}

    }

	private void merge(boolean cleanup) {
		if(intermediateList.size()>=4) {
			System.out.println("merge");
			RandomAccessFile firstDict = intermediateList.removeFirst();
			RandomAccessFile firstData = intermediateList.removeFirst();

			RandomAccessFile secondDict = intermediateList.removeFirst();
			RandomAccessFile secondData = intermediateList.removeFirst();

			
			try {
				RandomAccessFile resultDict;
				RandomAccessFile resultData;
				if (cleanup) {
					resultDict = new RandomAccessFile( INDEXDIR + "/" + DICTIONARY_FNAME, "rw" );
					resultData = new RandomAccessFile( INDEXDIR + "/" + DATA_FNAME, "rw" );
					dictionaryFile =resultDict;
					dataFile = resultData;
					System.out.println("resultDict final" + resultDict);
				}
				else {
					intermediateCounter++;
					System.out.println("intermediateCounter" + intermediateCounter);
					String tempNameDict = INDEXDIR + "/" + INTERMEDIATE_DICTIONARY_FNAME + intermediateCounter;
					resultDict = new RandomAccessFile( tempNameDict, "rw" );
					System.out.println("tempName merge " + tempNameDict);
					String tempNameData = INDEXDIR + "/" + INTERMEDIATE_DATA_FNAME + intermediateCounter;
					resultData = new RandomAccessFile( tempNameData, "rw" );
					System.out.println("tempName merge " + tempNameData);
				}

				mergeContents(resultDict,firstDict,secondDict,resultData,firstData,secondData, resultData); //todo



				if (!cleanup) {
					intermediateList.add(resultDict);
					intermediateList.add(resultData);
				}
			} catch (IOException e) {
				e.printStackTrace();
			}

		}
	}
	



	private void mergeContents(RandomAccessFile resultDict, RandomAccessFile firstDict, RandomAccessFile secondDict,
			RandomAccessFile resultData, RandomAccessFile firstData, RandomAccessFile secondData,
			RandomAccessFile resultDataFile) throws IOException {
		long resultDataPtr = 0;
		LinkedList<Long> alreadyMerged1 = new LinkedList<Long>();
		LinkedList<Long> alreadyMerged2 = new LinkedList<Long>();
		for(long ind = 0; ind <= TABLESIZE; ind++) {
			System.out.println(ind);
			Entry entry1 = null;
			Entry entry2 = null;
			
//			try {
//			try {
//			Entry e = readEntry(10, firstDict);	
//		}   
//		catch (EOFException e) {
//			System.out.println("EOFE at 10");
//		}
//		try {
//			Entry e = readEntry(11, firstDict);	
//		}   
//		catch (EOFException e) {
//			System.out.println("EOFE at 11");
//		}
//		try {
//			Entry e = readEntry(12, firstDict);	
//		}   
//		catch (EOFException e) {
//			System.out.println("EOFE at 12");
//		}
//		try {
//			Entry e = readEntry(13, firstDict);	
//		}   
//		catch (EOFException e) {
//			System.out.println("EOFE at 13");
//		}
//		try {
//			Entry e = readEntry(14, firstDict);	
//		}   
//		catch (EOFException e) {
//			System.out.println("EOFE at 14");
//		}
//		TimeUnit.SECONDS.sleep(1);
//	} catch (InterruptedException e) {
//		// TODO Auto-generated catch block
//		e.printStackTrace();
//	}
			
			try {
				entry1 = readEntry(ind, firstDict);	
			}   
			catch (EOFException e) {
				System.out.println("EOFE 1");
				entry1= null;
			}
	    	catch (IOException e) {
				e.printStackTrace();
	    	}
			try {
				entry2 = readEntry(ind, secondDict);
				
			}   
			catch (EOFException e) {
				System.out.println("EOFE 2");
				entry2= null;
			}
	    	catch (IOException e) {
				e.printStackTrace();
	    	}
			
			if (entry1!= null) {
				System.out.println("entry1.dataSize" + entry1.dataSize);
				System.out.println("entry1.dataPtr" + entry1.dataPtr);
			}
			if (entry2!= null) {
				System.out.println("entry2.dataSize" + entry2.dataSize);
				System.out.println("entry2.dataPtr" + entry2.dataPtr);
			}
			
			
			wirteMergedEntry(entry1, entry2, resultDict, ind, resultDataPtr);
			
			
			if (entry1!= null && entry2!=null && entry1.checksum == entry2.checksum) { // common case
				if (entry1.dataSize== 0 && entry2.dataSize== 0) {
					System.out.println("both empty");
				}else {
					System.out.println("cs match");
				}
				
				resultDataPtr+= writeCorrespondingEntries(entry1, entry2, firstData, secondData, resultDataPtr ,
						ind, resultDict, resultDataFile);

			}
			else { // this happens due to collisions
				System.out.println("collision handling");
				if (entry1!=null && entry1.dataSize!= 0) {
					System.out.println("case1");
					resultDataPtr+=findCorresponding(alreadyMerged1, ind, entry1,resultDataPtr, alreadyMerged2, firstData,
							secondData, secondDict, resultDataFile);
				}
				if (entry2!=null && entry2.dataSize!= 0) {
					System.out.println("case2");
					resultDataPtr+=findCorresponding(alreadyMerged2, ind, entry2,resultDataPtr, alreadyMerged1, secondData,
							firstData, firstDict, resultDataFile);
				}
			}
			
		}
	}



	private void wirteMergedEntry(Entry entry1, Entry entry2, RandomAccessFile resultDict, long ind, long resultDataPtr) {
		System.out.println("wirteMergedEntry called");
		
		
		
		if ((entry1!=null && entry1.dataSize!= 0) && (entry2!=null && entry2.dataSize!= 0)) {
			System.out.println("writing entry a");
			int resultDataSize = entry1.dataSize+entry2.dataSize;
			Entry mergedEntry = new Entry(entry1.checksum,resultDataPtr,resultDataSize);
			writeEntry(mergedEntry, ind, resultDict);
			
		}
		if ((entry1==null || entry1.dataSize== 0) && (entry2!=null && entry2.dataSize!= 0)) {
			System.out.println("writing entry b");
			Entry mergedEntry = new Entry(entry2.checksum,resultDataPtr,entry2.dataSize);
			writeEntry(mergedEntry, ind, resultDict);
		}
		if ((entry1!=null && entry1.dataSize!= 0) && (entry2==null || entry2.dataSize== 0)) {
			System.out.println("writing entry c");
			Entry mergedEntry = new Entry(entry1.checksum,resultDataPtr,entry1.dataSize);
			writeEntry(mergedEntry, ind, resultDict);
		}

	}



	private int writeCorrespondingEntries(Entry entry1, Entry entry2, RandomAccessFile firstData,
			RandomAccessFile secondData, long resultDataPtr, long ind, RandomAccessFile resultDict,
			RandomAccessFile resultDataFile) {

		if ((entry1==null || entry1.dataSize== 0) && (entry2!= null && entry2.dataSize!= 0)) {
			PostingsList pList = PostingsList.stringToObj(readData( entry2.dataPtr, entry2.dataSize, firstData));
			System.out.println("writing to merge result 1" + pList.toString());
			return writeData( pList.toString(), resultDataPtr, resultDataFile);
		}
		if ((entry1!= null && entry1.dataSize!= 0) && (entry2==null || entry2.dataSize== 0)) {
			PostingsList pList = PostingsList.stringToObj(readData( entry1.dataPtr, entry1.dataSize, secondData));
			System.out.println("writing to merge result 2" + pList.toString());
			return writeData( pList.toString(), resultDataPtr, resultDataFile);
		}
		if ((entry1!=null && entry1.dataSize!= 0) && (entry2!=null && entry2.dataSize!= 0)) {
			return writeMergedData(entry1,entry2, ind, resultDataPtr, firstData, secondData, resultDataFile); // actual merge of two dict entries
		}
		return 0; // both entries invalid
		
	}



	private int findCorresponding(LinkedList<Long> alreadyMergedToCheck, long ind, Entry entry, long resultDataPtr,
			LinkedList<Long> alreadyMergedToUpdate,RandomAccessFile dataFile1, RandomAccessFile dataFile2,
			RandomAccessFile otherDict, RandomAccessFile resultDataFile) {
		System.out.println("alreadyMergedToCheck" +  alreadyMergedToCheck);
//		for (int i = 0; i < alreadyMergedToUpdate.size(); i++) {
//			if(alreadyMergedToUpdate.get(i) < ind) {
//				alreadyMergedToUpdate.remove(ind);
//			}
//		}
		if (!alreadyMergedToCheck.contains(ind)) {
			Entry entryMatch = readEntryAndCheck(ind+1, entry.checksum,otherDict );
			if (entryMatch!= null) {
				alreadyMergedToUpdate.add(entryMatch.index);
				System.out.println("alreadyMergedToUpdate" +  alreadyMergedToUpdate);
				return writeCorrespondingEntries(entry, entryMatch, dataFile1, dataFile2,
						resultDataPtr , ind, resultDataFile, resultDataFile);
			}
			else {
				PostingsList pList = PostingsList.stringToObj(readData( entry.dataPtr, entry.dataSize, dataFile1));
				System.out.println("writing to merge result 3" + pList.toString());
				return writeData( pList.toString(), resultDataPtr, resultDataFile);
			}
//			resultDataPtr+= writeMergedEntry(entry,entryMatch, ind, resultDataPtr, dataFile1, dataFile2);
			
		}
		else return 0;

		
	}



	private int writeMergedData(Entry entry1, Entry entry2, long ind, long dataPtr, RandomAccessFile firstData,
			RandomAccessFile secondData, RandomAccessFile resultDataFile ) {
		int resultDataSize = entry1.dataSize+entry2.dataSize;
		
		
//		Entry mergedEntry = new Entry(entry1.checksum,dataPtr,resultDataSize);
//		writeEntry(mergedEntry, ind, resultDict);
//		System.out.println("readData( entry1.dataPtr, entry1.dataSize)" + readData( entry1.dataPtr, entry1.dataSize));
//		System.out.println("dataFile1" + dataFile1);
//		System.out.println("readData( entry2.dataPtr, entry2.dataSize)" + readData( entry2.dataPtr, entry2.dataSize));
//		System.out.println("dataFile2" + dataFile2);
		

		PostingsList pList1 = PostingsList.stringToObj(readData( entry1.dataPtr, entry1.dataSize, firstData));
		PostingsList pList2 = PostingsList.stringToObj(readData( entry2.dataPtr, entry2.dataSize, secondData));
		PostingsList pList3 = mergePlists(pList1, pList2);
		System.out.println("writing to merge result 4" + pList3.toString());
		writeData( pList3.toString(), dataPtr,resultDataFile);
		return resultDataSize;
	}



	private PostingsList mergePlists(PostingsList pList1, PostingsList pList2) {
		PostingsList result = PostingsList.merge(pList1,pList2);
		System.out.println("merging plists " + pList1 + " and " + pList2 + " to" + result);
		
		return result;
	}



	public void cleanup() {
		System.out.println("cleanup");
        System.err.println( index.keySet().size() + " unique words" );
        System.err.print( "Writing index to disk..." );
        try {
        	dataWritePtr = 0;
			writeIndex();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        intermediateList.add(dictionaryFile);
        intermediateList.add(dataFile);
        merge(true);
        
        try {
            readDocInfo();
        } catch ( FileNotFoundException e ) {
        	
        } catch ( IOException e ) {
            e.printStackTrace();
        }
        // wait for threads to finish
        System.err.println( "done!" );
    }
	
    protected void readDocInfo() throws IOException {
        File file = new File( INDEXDIR + "/docInfo" );
        FileReader freader = new FileReader(file);
        try (BufferedReader br = new BufferedReader(freader)) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] data = line.split(";");
                docNames.put(new Integer(data[0]), data[1]+5);
                docLengths.put(new Integer(data[0]), new Integer(data[2]+5));
            }
        }
        freader.close();
    }
    
    

	
	
	
	
	
	
	
	
	
	
	
	
	
	
}
