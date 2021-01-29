/*  
 *   This file is part of the computer assignment for the
 *   Information Retrieval course at KTH.
 * 
 *   Johan Boye, 2017
 */  

package ir;

import java.util.ArrayList;

public class PostingsList {
    
    /** The postings list */
    private ArrayList<PostingsEntry> list = new ArrayList<PostingsEntry>();


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
    	System.out.println(representation);

    	PostingsList result = new PostingsList();
    	String delims = "[;]";
    	String[] eStrings = representation.split(delims);
    	for (String eString: eStrings) {
    		result.append(PostingsEntry.stringToObj(eString));
    	}
		return result;
    } 

    // 
    //  YOUR CODE HERE
    //
}





















