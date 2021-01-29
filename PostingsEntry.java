/*  
 *   This file is part of the computer assignment for the
 *   Information Retrieval course at KTH.
 * 
 *   Johan Boye, 2017
 */  

package ir;

import java.util.HashMap;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.io.Serializable;
import java.util.*; 

public class PostingsEntry implements Comparable<PostingsEntry>, Serializable {

    public int docID;
    public double score = 0;
    public List<Integer> offsetList=new ArrayList<Integer>();

    /**
     *  PostingsEntries are compared by their score (only relevant
     *  in ranked retrieval).
     *
     *  The comparison is defined so that entries will be put in 
     *  descending order.
     */
    public int compareTo( PostingsEntry other ) {
       return Double.compare( other.score, score );
    }
    
    void addToOffsetList(int offset) {
    	offsetList.add(offset);
    }
    
    public String toString() { 
    	String representation="";
    	representation = representation+docID+":";
    	representation = representation+score+":";
    	for (int off : offsetList)
    		representation = representation+off +",";
		return representation;
    } 
    
    public static PostingsEntry stringToObj(String representation) {
    	PostingsEntry result = new PostingsEntry();
    	String delims1 = "[:]";
    	String[] split1 = representation.split(delims1);
//    	System.out.println(representation);
		result.docID = Integer.parseInt(split1[0]);
		result.score = Double.parseDouble(split1[1]);
    	String delims2 = "[,]";
    	String[] offsetListString = split1[2].split(delims2);
    	for (String intString: offsetListString) {
    		result.addToOffsetList(Integer.parseInt(intString));
    	}
		return result;
    } 
    
    
//	PostingsList result = new PostingsList();
//	String delims = "[,]";
//	String[] eStrings = representation.split(delims);
//	for (String eString: eStrings) {
//		result.append(PostingsEntry.stringToObj(eString));
//	}
//	return result;

    //
    // YOUR CODE HERE
    //
}

