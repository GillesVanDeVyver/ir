/*  
 *   This file is part of the computer assignment for the
 *   Information Retrieval course at KTH.
 * 
 *   Johan Boye, 2017
 */  

package ir;


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
    
    static List<Integer> mergeOffsetLists(List<Integer> offsetList1, List<Integer> offsetList2) {
    	List<Integer> newList = new ArrayList<Integer>();
    	if (offsetList2.get(0)> offsetList1.get(0)) {
        	newList.addAll(offsetList1);
        	newList.addAll(offsetList2);
    	}
    	else {
    		newList.addAll(offsetList2);
        	newList.addAll(offsetList1);
    	}

    	return newList;
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
		try {
    	PostingsEntry result = new PostingsEntry();
    	String delims1 = "[:]";
    	String[] split1 = representation.split(delims1);
		result.docID = Integer.parseInt(split1[0]);
		result.score = Double.parseDouble(split1[1]);
    	String delims2 = "[,]";
    	String[] offsetListString = split1[2].split(delims2);
    	for (String intString: offsetListString) {
    		result.addToOffsetList(Integer.parseInt(intString));
    	}
		return result;
		}
		catch (Exception e) {
			System.out.println("representation entry" + representation);
			throw e;
		}
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

