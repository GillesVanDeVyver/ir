/*  
 *   This file is part of the computer assignment for the
 *   Information Retrieval course at KTH.
 * 
 *   Johan Boye, 2017
 */  

package ir;

import java.util.ArrayList;
import java.util.List;

import ir.Query.QueryTerm;

/**
 *  Searches an index for results of a query.
 */
public class Searcher {

    /** The index to be searched by this Searcher. */
    Index index;

    /** The k-gram index to be searched by this Searcher */
    KGramIndex kgIndex;
    
    /** Constructor */
    public Searcher( Index index, KGramIndex kgIndex ) {
        this.index = index;
        this.kgIndex = kgIndex;
    }

    /**
     *  Searches the index for postings matching the query.
     *  @return A postings list representing the result of the query.
     */
    public PostingsList search( Query query, QueryType queryType, RankingType rankingType ) { 
    	ArrayList<QueryTerm> term = query.queryterm;
    	String s1 = term.get(0).term;
    	PostingsList list1 = index.getPostings(s1);
    	if (term.size()==1) {
    		return list1;
    	}
    	int i = 1;
		PostingsList result = list1;
		while(i<term.size()) {
			String s2 = term.get(i).term;
			PostingsList list2 = index.getPostings(s2);
	    	result = merge(result,list2,queryType);
			i++;
		}
        return result;
    }

	// money transfer gives 106 instead of 105 => something diff in regex patterns
	private PostingsList merge(PostingsList list1, PostingsList list2, QueryType queryType) {
		int ind1=0;
		int ind2=0;
    	PostingsList result = new PostingsList();
		while (ind1!=list1.size() && ind2!=list2.size()) {
			int id1 = list1.get(ind1).docID;
			int id2 = list2.get(ind2).docID;
			if (id1==id2) {
		    	switch(queryType) {
		    		case INTERSECTION_QUERY:
		    			result.append(list1.get(ind1));
		    		case PHRASE_QUERY:
		    			result = findConsecutive(list1.get(ind1),list2.get(ind2),result);
		    		default:
		    			break;
		    	}
				ind1++;
				ind2++;
			}
			else if(id1>id2) {
				ind2++;
			}
			else {
				ind1++;
			}	
		}
		return result;
	}

	private PostingsList findConsecutive(PostingsEntry e1, PostingsEntry e2, PostingsList result) {
		List<Integer> offsetList1 = e1.offsetList;
		List<Integer> offsetList2 = e2.offsetList;
		int ind1=0;
		int ind2=0;
		while (ind1!=offsetList1.size() && ind2!=offsetList2.size()) {
			int offset1 = offsetList1.get(ind1);
			int offset2 = offsetList2.get(ind2);

			if (offset1+1==offset2) {
				result.add(e1.docID, offset2);
				ind1++;
				ind2++;
			}
			else if(offset1+1>offset2) {
				ind2++;
			}
			else {
				ind1++;
			}	
		}
		return result;
	}
	
	
}















