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


public class PostingsList {
    
    /** The postings list */
    private LinkedList<PostingsEntry> list = new LinkedList<PostingsEntry>();


    /** Number of postings in this list. */
    public int size() {
    	return getList().size();
    }

    /** Returns the ith posting. */
    public PostingsEntry get( int i ) {
    	return getList().get( i );
    }
    
    public void append( PostingsEntry e ) {
    if (getList().size()!=0 && getList().get(getList().size()-1).docID==e.docID)
    	return;
    getList().add(e);
    }
    
    public void add( int docID , int offset) {
    	
		if (getList().size()!=0){
			PostingsEntry lastEntry = getList().get(getList().size()-1);
			if (lastEntry.docID==docID) {
				lastEntry.addToOffsetList(offset);
				return;
			}
		}
			PostingsEntry e = new PostingsEntry();
			e.docID=docID;
			e.addToOffsetList(offset);
			getList().add(e);
    }
    
    
    public void set( PostingsEntry e , int offset) {
    	getList().set(offset, e);
    }
    
    public String toString() { 
    	String representation="";
    	for (PostingsEntry e : getList())
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
		PostingsEntry lastEntry1 = pList1.get(pList1.size()-1);
		PostingsEntry firstEntry2 = pList2.get(0);
		PostingsList result = new PostingsList();
		result.setList(mergeLists(pList1, pList2));
		return result;
	}

	private static LinkedList<PostingsEntry> mergeLists(PostingsList pList1, PostingsList pList2) {

		LinkedList<PostingsEntry> newList = new LinkedList<PostingsEntry>();
    	newList.addAll(pList1.getList());
    	newList.addAll(pList2.getList());
    	newList.sort(new DocIdSorter());
    	mergeDuplicates(newList);
    	return newList;
		
	} 
	
	private static void mergeDuplicates(LinkedList<PostingsEntry> list) {
		int i = 0;
		while (i<list.size()-1) {
			PostingsEntry e1 = list.get(i);
			PostingsEntry e2 = list.get(i+1);
			if (e1.docID==e2.docID) {
				e1.offsetList = PostingsEntry.mergeOffsetLists(e1.offsetList, e2.offsetList);
				list.remove(i+1);
			}
			i++;
		}

		
	}

	public void sortList() {
		this.getList().sort(new DocIdSorter());
	}

	public LinkedList<PostingsEntry> getList() {
		return list;
	}
	
	public PostingsList getCopy() {
		PostingsList result = new PostingsList();
		result.list = this.getListCopy();
		return result;
	}
	
	public LinkedList<PostingsEntry> getListCopy() {
		LinkedList<PostingsEntry> result = new LinkedList<PostingsEntry>();
		for (PostingsEntry e : list) {
			result.add(e.getCopy());
		}
		return result;
	}

	public void setList(LinkedList<PostingsEntry> list) {
		this.list = list;
	}
	

    // 
    //  YOUR CODE HERE
    //
}





















