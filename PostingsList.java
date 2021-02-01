/*  
 *   This file is part of the computer assignment for the
 *   Information Retrieval course at KTH.
 * 
 *   Johan Boye, 2017
 */  

package ir;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import com.google.common.collect.ObjectArrays;

public class PostingsList {
    
    /** The postings list */
    private LinkedList<PostingsEntry> list = new LinkedList<PostingsEntry>();


    /** Number of postings in this list. */
    public int size() {
    	return list.size();
    }

    /** Returns the ith posting. */
    public PostingsEntry get( int i ) {
    	return list.get( i );
    }
    
    public void append( PostingsEntry e ) {
    if (list.size()!=0 && list.get(list.size()-1).docID==e.docID)
    	return;
    list.add(e);
    }
    
    public void add( int docID , int offset) {
    	
		if (list.size()!=0){
			PostingsEntry lastEntry = list.get(list.size()-1);
			if (lastEntry.docID==docID) {
				lastEntry.addToOffsetList(offset);
				return;
			}
		}
			PostingsEntry e = new PostingsEntry();
			e.docID=docID;
			e.addToOffsetList(offset);
			list.add(e);
    }
    
    
    public void set( PostingsEntry e , int offset) {
    	list.set(offset, e);
    }
    
    public String toString() { 
    	String representation="";
    	for (PostingsEntry e : list)
    		representation = representation+e.toString()+";";
		return representation;
    } 
    
    public static PostingsList stringToObj(String representation) {
    	try {
    	PostingsList result = new PostingsList();
    	String delims = "[;]";
    	String[] eStrings = representation.split(delims);
    	for (String eString: eStrings) {
    		
    		result.append(PostingsEntry.stringToObj(eString));
    	}
    	return result;
    	}
		catch (Exception e) {
			System.out.println("representation list" + representation);
			throw e;
		}
		
    }

	public static PostingsList merge(PostingsList pList1, PostingsList pList2) {
//		System.out.println("pList1.list " + pList1.list);
//		System.out.println("pList2.list " + pList2.list);
		PostingsEntry lastEntry1 = pList1.get(pList1.size()-1);
		PostingsEntry firstEntry2 = pList2.get(0);
		boolean skipFirst = false;
		if (lastEntry1.docID==firstEntry2.docID) {
			lastEntry1.offsetList = PostingsEntry.mergeOffsetLists(lastEntry1.offsetList, firstEntry2.offsetList);
			skipFirst = true;
		}
		PostingsList result = new PostingsList();
		result.list =  mergeLists(pList1, pList2, skipFirst);
//		System.out.println("result " + result);
//		System.out.println("pList1 " + pList1);
//		System.out.println("pList2 " + pList2);
		return result;
	}

	private static LinkedList<PostingsEntry> mergeLists(PostingsList pList1, PostingsList pList2, boolean skipFirst) {
		LinkedList<PostingsEntry>  pList2ToMerge = (LinkedList<PostingsEntry>) pList2.list.clone();
		if (skipFirst) {
			pList2ToMerge.remove();
		}
		LinkedList<PostingsEntry> newList = new LinkedList<PostingsEntry>();
    	newList.addAll(pList1.list);
    	newList.addAll(pList2ToMerge);
//    	System.out.println("newList " + newList);

    	return newList;
		
	} 
	

    // 
    //  YOUR CODE HERE
    //
}





















