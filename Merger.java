package ir;

import java.io.EOFException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.LinkedList;

import ir.PersistentHashedIndex.Entry;

public class Merger {

	private RandomAccessFile primaryDict;
	private RandomAccessFile primaryData;
	private RandomAccessFile secondaryDict;
	private RandomAccessFile secondaryData;
//	private RandomAccessFile resultDict;
//	private RandomAccessFile resultData;
    private LinkedList<RandomAccessFile> mergeQueue = new LinkedList<RandomAccessFile>();
    private LinkedList<mergedEntry> entryQueue = new LinkedList<mergedEntry>();
//    private long dataWritePtr = 0;
    private PersistentScalableHashedIndex resultHashedIndex;
    
    
    public class mergedEntry {
        long checksum;
        String dataToWrite;

        
        public mergedEntry(long checksum, String dataToWrite) {
        	this.checksum = checksum;
        	this.dataToWrite =dataToWrite;
        }
    }
    
	public Merger(RandomAccessFile primaryDict, RandomAccessFile primaryData,
			RandomAccessFile secondaryDict, RandomAccessFile secondaryData,
			RandomAccessFile resultDict, RandomAccessFile resultData) {
		this.primaryDict = primaryDict;
		this.primaryData = primaryData;
		this.secondaryDict = secondaryDict;
		this.secondaryData = secondaryData;

		this.resultHashedIndex = new PersistentScalableHashedIndex(resultDict,resultData);

//		System.out.println("customread(resultData))" + customread(resultData));
//		System.out.println("customread(resultDict))" + customread(resultDict));
		
		
	}



	public void merge() {
		int ind = 0;
		boolean endOfFile1 = false;
		boolean endOfFile2 = false;
		while(!endOfFile1 || !endOfFile2) {
//			System.out.println(ind);
			int readDataSize1 =0;
			long readChecksum1 =0;
			long readDataPtr1 = -1;
			int readDataSize2 =0;
			long readChecksum2 =0;
			long readDataPtr2 = -1;
			if (!endOfFile1) {
				try { // read from primary
					
					primaryDict.seek( PersistentHashedIndex.ptrFromIndex(ind) );
			        readDataPtr1 = primaryDict.readLong();
			        readChecksum1 = primaryDict.readLong();
			        readDataSize1 = primaryDict.readInt();
			        
				} catch (EOFException e) {
					endOfFile1 = true;
					readDataSize1 = 0;
					System.out.println("EOF1" + ind);
					
				}
				catch (IOException e) {
					e.printStackTrace();
				}
			}
			if (!endOfFile2) {
				try { // read from secondary
//					System.out.println(ind);
					secondaryDict.seek( PersistentHashedIndex.ptrFromIndex(ind) );
			        readDataPtr2 = secondaryDict.readLong();
			        readChecksum2 = secondaryDict.readLong();
			        readDataSize2 = secondaryDict.readInt();
				} catch (EOFException e) {
					endOfFile2 = true;
					readDataSize2 = 0;
					System.out.println("EOF2" + ind);
				}
				catch (IOException e) {
					e.printStackTrace();
				}
			}
			
			if(readDataSize1==0 && readDataSize2==0)
				case1(ind);
			else if (readDataSize1!=0 && readDataSize2==0)
				case2(readDataSize1,readChecksum1,readDataPtr1,ind, true);
			else if (readDataSize1==0 && readDataSize2!=0)
				case2(readDataSize2,readChecksum2,readDataPtr2,ind, false);
			else if (readDataSize1!=0 && readDataSize2!=0) {
				if (readChecksum1==readChecksum2)
					case3(readDataSize1, readDataPtr1,
						  readDataSize2,readDataPtr2, readChecksum1, ind);
				else
					case4(readDataSize1,readChecksum1,readDataPtr1,
						  readDataSize2,readChecksum2,readDataPtr2, ind);
			}
			ind++;	
		}
		while(entryQueue.size()!=0) {
			case1(ind);
			ind++;
		}
	}

	private void case1(long ind) { // write first el in entryQueue if not empty
//		System.out.println("case1");
		if (entryQueue.size()>0) {
			mergedEntry toWrite = entryQueue.removeFirst();
			writeEntryAndData(toWrite, ind);
		}
	}
	
	private void case2(int readDataSize, long readChecksum, long readDataPtr, long ind, boolean first) { // find corresponding, merge, write at current ind
//		System.out.println("case2");	
//		System.out.println("first" + first);
		RandomAccessFile dictFileToLook;
		RandomAccessFile dataFileToLook;
		RandomAccessFile dataFileThis;
		if (first) {
			dataFileThis = primaryData;
			dictFileToLook = secondaryDict;
			dataFileToLook = secondaryData;
		}
			
		else {
			dataFileThis = secondaryData;
			dictFileToLook = primaryDict;
			dataFileToLook = primaryData;
		}
		mergedEntry merged = findAndDelCorresponding(ind+1,readChecksum,readDataPtr,readDataSize, dataFileThis, dataFileToLook, dictFileToLook);
		writeEntryAndData(merged, ind);
	}

	private void case3(int readDataSize1, long readDataPtr1,
			int readDataSize2, long readDataPtr2, long checksum, long ind) { // merge entries, write at current ind
//		System.out.println("case3");
		mergedEntry merged = mergeEntries(readDataSize1,readDataPtr1,readDataSize2,readDataPtr2, checksum,
				primaryData, secondaryData);
		writeEntryAndData(merged, ind);
	}

	// for 1 : find corresponding, merge, write at current ind (case2)
	// for 2 : find corresponding, merge add to entryQueue
	private void case4(int readDataSize1, long readChecksum1, long readDataPtr1, 
			int readDataSize2, long readChecksum2, long readDataPtr2, long ind) {
//		System.out.println("case4");
		case2(readDataSize1,readChecksum1,readDataPtr1,ind, true);
		mergedEntry merged = findAndDelCorresponding(ind+1,readChecksum2,readDataPtr2,readDataSize2,secondaryData, primaryData, primaryDict);
		entryQueue.add(merged);
	}
	
	private mergedEntry findAndDelCorresponding(long ind, long readChecksum, long readDataPtr, int readDataSize,
			RandomAccessFile dataFileThis, RandomAccessFile dataFileToLook, RandomAccessFile dictFileToLook) {
		mergedEntry result;
		Entry entryMatch = resultHashedIndex.readEntryAndDel(ind, readChecksum,dictFileToLook);
		if (entryMatch!=null) {
			result = mergeEntries(readDataSize,readDataPtr,entryMatch.dataSize,entryMatch.dataPtr,readChecksum,
					dataFileThis, dataFileToLook);
//			System.out.println("match found");
		}
		else {
//			System.out.println("no matach found");
//			System.out.println("readDataPtr" + readDataPtr);
//			System.out.println("readDataSize" + readDataSize);
//			System.out.println("dataFileThis" + dataFileThis);
//			System.out.println("customread(dataFileThis))" + customread(dataFileThis));
			String s1 = PersistentHashedIndex.readData( readDataPtr, readDataSize, dataFileThis );
//			System.out.println("s1" + s1);
			result = new mergedEntry(readChecksum,s1);
		}
		return result;
	}
	
	
	
	private mergedEntry mergeEntries(int readDataSize1, long readDataPtr1, int readDataSize2, long readDataPtr2, long checksum,
			RandomAccessFile dataFileThis, RandomAccessFile dataFileToLook) {
		String s1 = PersistentHashedIndex.readData( readDataPtr1, readDataSize1,  dataFileThis);
		PostingsList pList1 = PostingsList.stringToObj(s1);
		String s2 = PersistentHashedIndex.readData( readDataPtr2, readDataSize2,  dataFileToLook);
		PostingsList pList2 = PostingsList.stringToObj(s2);
		PostingsList pList3 = PostingsList.merge(pList1, pList2);
//		System.out.println("pList3.toString()" + pList3.toString());
		return new mergedEntry(checksum, pList3.toString());
	}
	
	private void writeEntryAndData(mergedEntry merged, long ind) {
//		System.out.println("merged.dataToWrite" + merged.dataToWrite);
		int dataSizeWritten =resultHashedIndex.writeData( merged.dataToWrite, resultHashedIndex.dataWritePtr);
//		System.out.println("dataSizeWritten" + dataSizeWritten);
//		System.out.println("resultHashedIndex.dataWritePtr" + resultHashedIndex.dataWritePtr);

		Entry eToWrite = resultHashedIndex.new Entry(merged.checksum, resultHashedIndex.dataWritePtr,dataSizeWritten);
		
		resultHashedIndex.writeEntry(eToWrite, ind );
		
//		System.out.println("eToWrite.dataPtr" + eToWrite.dataPtr);
//		System.out.println("eToWrite.checksum" + eToWrite.checksum);
//		System.out.println("eToWrite.dataSize" + eToWrite.dataSize);
    	try {
    		resultHashedIndex.dictionaryFile.seek( PersistentHashedIndex.ptrFromIndex(ind) );
    	}
        catch ( Exception e ) {
        	System.err.println("eeeeeeeeeeeeeeeeeee6");
        }
//    	System.out.println("ok1");
        try {
			long readDataPtr = resultHashedIndex.dictionaryFile.readLong();
		} catch (IOException e) {
			System.err.println("eeeeeeeeeeeeeeeeeee7");
		}
//        System.out.println("ok2");
        
        
        
        
		resultHashedIndex.dataWritePtr += dataSizeWritten;
//		if(dataSizeWritten!=0)
//			assert (false);
//		if(dataSizeWritten==0)
//			assert (false);
//		System.out.println("resultHashedIndex.dataWritePtr after" + resultHashedIndex.dataWritePtr);
		// check writing
//		readEntryToCheck(ind,merged.checksum,resultHashedIndex.dictionaryFile, resultHashedIndex.dataFile);
	}
	
    void readEntryToCheck( long index , long checksum, RandomAccessFile dictfFile, RandomAccessFile dataFile) {  
    	long readDataPtr = -2;
        try {
        	try {
        		dictfFile.seek( PersistentHashedIndex.ptrFromIndex(index) );
        	}
        	
            catch ( EOFException e ) {
            	System.err.println("eeeeeeeeeeeeeeeeeee5");
            }
//        	System.out.println("ok1");
            readDataPtr = dictfFile.readLong();
//            System.out.println("ok2");
            long readChecksum = dictfFile.readLong();
//            System.out.println("ok3");
            int readDataSize = dictfFile.readInt();
//            System.out.println("ok4");
            if (readDataSize == 0) {
            	System.err.println("eeeeeeeeeeeeeeeeeee1");
            }
            if (readChecksum!=checksum) { //checksum didn't match
            	long newIndex = index+1;
            	System.err.println("eeeeeeeeeeeeeeeeeee2");
            }
            else {
//            	System.out.println("readDataPtr2" + readDataPtr);
            	Entry dirEntry = resultHashedIndex.new Entry(checksum,readDataPtr,readDataSize, index);
            	String dataString = PersistentHashedIndex.readData( dirEntry.dataPtr, dirEntry.dataSize, dataFile);
//            	System.out.println("dataString" + dataString);
//            	System.out.println("dirEntry.dataPtr" + dirEntry.dataPtr);
//            	System.out.println("dirEntry.dataSize" + dirEntry.dataSize);
       		 	PostingsList.stringToObj(dataString);
            }
        } 
        catch ( EOFException e ) {
        	System.err.println("eeeeeeeeeeeeeeeeeee3");
        	System.out.println("readDataPtr" + readDataPtr);
        }catch ( IOException e ) {
            e.printStackTrace();
            System.err.println("eeeeeeeeeeeeeeeeeee4");
        }
    }
    


    String customread( RandomAccessFile file) {
//    	System.out.println("read");
        try {
        	file.seek( 0 );
            byte[] data = new byte[9];
            file.readFully( data );
            return new String(data);
        } catch ( IOException e ) {
//            e.printStackTrace();
            return null;
        }
    }

}





















