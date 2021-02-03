package ir;

import java.io.EOFException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.LinkedList;

import ir.PersistentHashedIndex.Entry;

public class Merger extends Thread{

	private RandomAccessFile primaryDict;
	private RandomAccessFile primaryData;
	private RandomAccessFile secondaryDict;
	private RandomAccessFile secondaryData;
    private LinkedList<RandomAccessFile> mergeQueue = new LinkedList<RandomAccessFile>();
    private LinkedList<mergedEntry> entryQueue = new LinkedList<mergedEntry>();
    private PersistentScalableHashedIndex resultHashedIndex;
	private LinkedList<Merger> threadQueue = new LinkedList<Merger>();
    
    /**
     *   A helper class representing a merged entry.
     *   The merged entry only has a checksum and the data to be written.
     */ 
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
			RandomAccessFile resultDict, RandomAccessFile resultData,
			LinkedList<RandomAccessFile>  mergeQueue,
			LinkedList<Merger>  threadQueue) {
		this.primaryDict = primaryDict;
		this.primaryData = primaryData;
		this.secondaryDict = secondaryDict;
		this.secondaryData = secondaryData;
		this.resultHashedIndex = new PersistentScalableHashedIndex(resultDict,resultData);
		this.mergeQueue = mergeQueue;
		this.threadQueue = threadQueue;
	
	}

    /**
     *  Goes through every index position and merge them into one dictionary with corresponing data.
     */
	public void run() {
		int ind = 0;
		boolean endOfFile1 = false;
		boolean endOfFile2 = false;
		while(!endOfFile1 || !endOfFile2) {
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
				}
				catch (IOException e) {
					e.printStackTrace();
				}
			}
			if (!endOfFile2) {
				try { // read from secondary
					secondaryDict.seek( PersistentHashedIndex.ptrFromIndex(ind) );
			        readDataPtr2 = secondaryDict.readLong();
			        readChecksum2 = secondaryDict.readLong();
			        readDataSize2 = secondaryDict.readInt();
				} catch (EOFException e) {
					endOfFile2 = true;
					readDataSize2 = 0;
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
		
		mergeQueue.add(resultHashedIndex.dictionaryFile);
		mergeQueue.add(resultHashedIndex.dataFile);
		synchronized ( threadQueue ) {
			threadQueue .remove(this);
		}
	}

    /**
     *  Writes the first entry in the entryQueue if any at this index.
     *  We do this when both entries are null.
     *  
     *  @param ind The index to write
     *
     */
	private void case1(long ind) { // write first el in entryQueue if not empty
		if (entryQueue.size()>0) {
			mergedEntry toWrite = entryQueue.removeFirst();
			writeEntryAndData(toWrite, ind);
		}
	}
	
    /**
     *  Writes an entry with given data to the given index.
     *  We do this when only one of the entries is not null
     *  
     *  @param readDataSize The dataSize of the entry
     *  @param readChecksum The checksum of the entry
     *  @param readDataPtr  The data pointer of the entry
     *  @param ind 			The index to write
     *  @param first 		Boolean telling which file this entry comes from
     *
     */
	private void case2(int readDataSize, long readChecksum, long readDataPtr, long ind, boolean first) { // find corresponding, merge, write at current ind
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

    /**
     *  Merges entries with given data to the given index.
     *  We do this when both entries are not null and the checksum matches.
     *  
     *  @param readDataSize1 	The dataSize of the first entry
     *  @param readDataPtr1 	The data pointer of the first entry
     *  @param readDataSize2  	The dataSize of the second entry
     *  @param readDataPtr2 	The data pointer of the second entry
     *  @param checksum 		The matching checksum
     *  @param ind 				The index to write
     *
     */
	private void case3(int readDataSize1, long readDataPtr1,
			int readDataSize2, long readDataPtr2, long checksum, long ind) { // merge entries, write at current ind
		mergedEntry merged = mergeEntries(readDataSize1,readDataPtr1,readDataSize2,readDataPtr2, checksum,
				primaryData, secondaryData);
		writeEntryAndData(merged, ind);
	}

    /**
     *  Find the corresponding entry for each of entries.
     *  Merge the corresponding entries.
     *  Write the merged entry originating from the first file to the given index.
     *  Add the merged entry originating from the second file to the the entryQueue.
     *  We do this when both entries are not null and the checksums don't match.
     *  
     *  @param readDataSize1 	The dataSize of the first entry
     *  @param readDataPtr1 	The data pointer of the first entry
     *  @param readChecksum1 	The checksum of the first entry
     *  @param readDataSize2  	The dataSize of the second entry
     *  @param readDataPtr2 	The data pointer of the second entry
     *  @param readChecksum2 	The checksum of the second entry
     *  @param ind 				The index to write
     *
     */
	private void case4(int readDataSize1, long readChecksum1, long readDataPtr1, 
			int readDataSize2, long readChecksum2, long readDataPtr2, long ind) {
		case2(readDataSize1,readChecksum1,readDataPtr1,ind, true);
		mergedEntry merged = findAndDelCorresponding(ind+1,readChecksum2,readDataPtr2,readDataSize2,secondaryData, primaryData, primaryDict);
		entryQueue.add(merged);
	}
	
    /**
     *  Finds the entry corresponding to the given data and merges it with the data.
     *  Deletes the found entry.
     *
     *  @param ind 				The index in the dictionary file where to start reading.
     *  @param readChecksum 	The checksum to check
     *  @param readDataPtr 		The data pointer of the entry to be matched
     *  @param readDataSize 	The data size of the entry to be matched
     *  @param dataFileThis 	The data file of the entry to be matched
     *  @param dataFileToLook 	The data file to look for a match
     *  @param dictFileToLook 	The dictionary file to look for a match
     *  
     *  @return The merged entry
     */
	private mergedEntry findAndDelCorresponding(long ind, long readChecksum, long readDataPtr, int readDataSize,
			RandomAccessFile dataFileThis, RandomAccessFile dataFileToLook, RandomAccessFile dictFileToLook) {
		mergedEntry result;
		Entry entryMatch = resultHashedIndex.readEntryAndDel(ind, readChecksum,dictFileToLook);
		if (entryMatch!=null) {
			result = mergeEntries(readDataSize,readDataPtr,entryMatch.dataSize,entryMatch.dataPtr,readChecksum,
					dataFileThis, dataFileToLook);
		}
		else {
			String s1 = PersistentHashedIndex.readData( readDataPtr, readDataSize, dataFileThis );
			result = new mergedEntry(readChecksum,s1);
		}
		return result;
	}
	
	
    /**
     *  Finds the entry corresponding to the given data and merges it with the data.
     *  Deletes the found entry.
     *
     *  @param readDataSize1 	The dataSize of the first entry
     *  @param readDataPtr1 	The data pointer of the first entry
     *  @param readDataSize2  	The dataSize of the second entry
     *  @param readDataPtr2 	The data pointer of the second entry
     *  @param checksum 		The matching checksum
     *  @param dataFileThis 	The data file of the first entry
     *  @param dataFileToLook 	The data file of the second entry
     *       *  
     *  @return The merged entry
     */
	private mergedEntry mergeEntries(int readDataSize1, long readDataPtr1, int readDataSize2, long readDataPtr2, long checksum,
			RandomAccessFile dataFileThis, RandomAccessFile dataFileToLook) {
		String s1 = PersistentHashedIndex.readData( readDataPtr1, readDataSize1,  dataFileThis);
		PostingsList pList1 = PostingsList.stringToObj(s1);
		String s2 = PersistentHashedIndex.readData( readDataPtr2, readDataSize2,  dataFileToLook);
		PostingsList pList2 = PostingsList.stringToObj(s2);
		PostingsList pList3 = PostingsList.merge(pList1, pList2);
		return new mergedEntry(checksum, pList3.toString());
	}
	
    /**
     *  Write the merged entry with corresponding data
     *  
     *  @param merged 	Merged entry to write
     *  @param ind 	index to write to
     *
     */
	private void writeEntryAndData(mergedEntry merged, long ind) {
		int dataSizeWritten =resultHashedIndex.writeData( merged.dataToWrite, resultHashedIndex.dataWritePtr);
		Entry eToWrite = resultHashedIndex.new Entry(merged.checksum, resultHashedIndex.dataWritePtr,dataSizeWritten);
		resultHashedIndex.writeEntry(eToWrite, ind );
		resultHashedIndex.dataWritePtr += dataSizeWritten;
	}
	

}





















