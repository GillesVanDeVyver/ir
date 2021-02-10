/*  
 *   This file is part of the computer assignment for the
 *   Information Retrieval course at KTH.
 * 
 *   Johan Boye, 2017
 */  

package ir;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;

import ir.Query.QueryTerm;

/**
 *  Searches an index for results of a query.
 */
public class Searcher {

    /** The index to be searched by this Searcher. */
    Index index;

    /** The k-gram index to be searched by this Searcher */
    KGramIndex kgIndex;
    
    double[] pageRankVector;
    
    Integer[] sortedPageRanks;

    
    /** Constructor */
    public Searcher( Index index, KGramIndex kgIndex ) {
        this.index = index;
        this.kgIndex = kgIndex;
        
        try {
        	RandomAccessFile outputFile = new RandomAccessFile( "./pageRankResult", "rw" );
    		outputFile.seek(0);
    		
    		int numberOfDocs = outputFile.readInt();
    		pageRankVector = new double[numberOfDocs];
        	for(int i = 0; i <numberOfDocs; i++){
        		pageRankVector[i] = outputFile.readDouble();
        	}
        	sortedPageRanks = argsort(pageRankVector, false);
        } catch ( IOException e ) {
            e.printStackTrace();
        }  
    }
    
    public static Integer[] argsort(final double[] a, final boolean ascending) {
        Integer[] indexes = new Integer[a.length];
        for (int i = 0; i < indexes.length; i++) {
            indexes[i] = i;
        }
        Arrays.sort(indexes, new Comparator<Integer>() {
            @Override
            public int compare(final Integer i1, final Integer i2) {
                return (ascending ? 1 : -1) * Float.compare((float) a[i1], (float) a[i2]);
            }
        });
        return (indexes);
    }
    


    /**
     *  Searches the index for postings matching the query.
     *  @return A postings list representing the result of the query.
     */
    public PostingsList search( Query query, QueryType queryType, RankingType rankingType ) { 
    	ArrayList<QueryTerm> term = query.queryterm;
		if (queryType==queryType.INTERSECTION_QUERY || queryType==queryType.PHRASE_QUERY) {
			return unRankedsearch(term, query,queryType );
		}
		else {
			return rankedsearch(term, query,rankingType );
		}

    }
    

	private PostingsList rankedsearch(ArrayList<QueryTerm> term, Query query , RankingType rankingType) {
		PostingsList result = new PostingsList();
    	int N = Index.docLengths.size();
    	int i = 0;
		while(i<term.size()) {
			String token = term.get(i).term;
			PostingsList pList = index.getPostings(token);
	    	LinkedList<PostingsEntry> eList = pList.getList();
	    	for (PostingsEntry e : eList){

	        	switch(rankingType) {
					case TF_IDF:
			    		// tf = how many times the term appears in the doc
			    		int tf = e.offsetList.size();
			    		Integer docLength = Index.docLengths.get(e.docID);
			    		float frac = (float) N/eList.size();
			    		double idf = Math.log(frac);
			    		// multiply tf by idf gives weight for term
			    		e.score = tf*idf/docLength;
			    		// for task 2.3
	//		    		e.score = idf;
	//		    		System.out.println("N" + N);
	//		    		System.out.println("eList.size()"+eList.size());
	//		    		System.out.println("frac"+frac);
			    		break;
					case PAGERANK:
						e.score = pageRankVector[e.docID];
						break;
					case COMBINATION:
						double w1 = 1;
						double w2 = 2000;
			    		int tfc = e.offsetList.size();
			    		Integer docLengthc = Index.docLengths.get(e.docID);
			    		float fracc = (float) N/eList.size();
			    		double idfc = Math.log(fracc);
			    		e.score = w1*tfc*idfc/docLengthc+w2*pageRankVector[e.docID];
	        	}
	    	}
	    	if (i==0) {
	    		result = pList;
	    	}
	    	else{
		    	result = rankedmMerge(result,pList);
	    	}
		i++;
		}
		java.util.Collections.sort(result.getList());
		return result;
	}

	private PostingsList rankedmMerge(PostingsList pList1, PostingsList pList2) {
		int ind1=0;
		int ind2=0;
		PostingsList result = new PostingsList();
		while (ind1!=pList1.size() || ind2!=pList2.size()) {
			int id1;
			if (ind1 == pList1.size()) {
				id1 = Integer.MAX_VALUE;
			}
			else {
				id1 = pList1.get(ind1).docID;
			}
			int id2;
			if (ind2 == pList2.size()) {
				id2 = Integer.MAX_VALUE;
			}
			else {
				id2 = pList2.get(ind2).docID;
			}
			PostingsEntry mergedEntry = new PostingsEntry();
			if (id1==id2) {
				mergedEntry.score = pList1.get(ind1).score + pList2.get(ind2).score;
				mergedEntry.docID = id1;
				ind1++;
				ind2++;
			}
			else if(id1>id2) {
				mergedEntry.score = pList2.get(ind2).score;
				mergedEntry.docID = id2;
				ind2++;
			}
			else {
				mergedEntry.score = pList1.get(ind1).score;
				mergedEntry.docID = id1;
				ind1++;
			}	
			result.append(mergedEntry);
		}
		return result;
	}

	private PostingsList unRankedsearch(ArrayList<QueryTerm> term, Query query, QueryType queryType) {
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
	    	result = mergeQueryUnranked(result,list2,queryType);
			i++;
		}
		
        return result;
	}

	private PostingsList mergeQueryUnranked(PostingsList list1, PostingsList list2, QueryType queryType) {
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

//	private PostingsList rankedQuery(PostingsList list1, PostingsList list2, QueryType queryType) {
//		PostingsEntry term = list1.get(0);
//    	for (Entry<Integer, Integer> entry : Index.docLengths.entrySet()) {
//    	    Integer docID = entry.getKey();
//    	    Integer docLength = entry.getValue();
//    	    
//    	}
//		
//		return null;
//	}

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















