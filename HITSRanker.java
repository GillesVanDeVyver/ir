/**
 *   Computes the Hubs and Authorities for an every document in a query-specific
 *   link graph, induced by the base set of pages.
 *
 *   @author Dmytro Kalpakchi
 */

package ir;

import java.util.*;
import java.util.Map.Entry;
import java.io.*;


public class HITSRanker {

    /**
     *   Max number of iterations for HITS
     */
    final static int MAX_NUMBER_OF_STEPS = 1000;

    /**
     *   Convergence criterion: hub and authority scores do not 
     *   change more that EPSILON from one iteration to another. (For 10 consecutive iterations)
     */
    final static double EPSILON = 0.001;

    /**
     *   The inverted index
     */
    Index index;

    /**
     *   Mapping from the titles to internal document ids used in the links file
     */
    HashMap<String,Integer> titleToId = new HashMap<String,Integer>();
    
    /**
     *   Mapping from the internal document ids to titles
     */
    HashMap<Integer,String> IDToTitle = new HashMap<Integer,String>();

    /**
     *  Reciprocal of PageRank.links
     */
    HashMap<Integer,HashMap<Integer,Boolean>> reversedLink = new HashMap<Integer,HashMap<Integer,Boolean>>();

    /**
     *   Sparse vector containing hub scores
     */
    HashMap<Integer,Double> hubs;

    /**
     *   Sparse vector containing authority scores
     */
    HashMap<Integer,Double> authorities;
    
    /**
     *   PageRank object to reuse code
     */
    PageRank PR;

    /**
     *   A helper class representing the root and base set of a query
     */ 
    public class HITSSet {
    	Set<Integer> baseIDs;
    	Set<Integer> rootIDs;
    	
        /**
         *  Constructor. Construct the base set and root set from given titles
         *  
         *  @param titles The titles of the query
         */
    	public HITSSet(String[] titles) {
        	baseIDs = new HashSet<Integer>();
        	rootIDs = new HashSet<Integer>();

        	for (String title: titles) {
        		Integer rootID = titleToId.get(title);
        		if (rootID!= null) {
        			baseIDs.add(PR.docNumber.get(String.valueOf(rootID)));
        			rootIDs.add(PR.docNumber.get(String.valueOf(rootID)));
    				HashMap<Integer, Boolean> outLinks = PR.link.get(rootID);
    				if (outLinks !=null) {
    		        	for (Entry<Integer, Boolean> entryOut : outLinks.entrySet()) {
    		        		baseIDs.add(entryOut.getKey());
    		        	}
    				}
    		  		HashMap<Integer, Boolean> inLinks = reversedLink.get(rootID);
    				if (inLinks !=null) {
    		        	for (Entry<Integer, Boolean> entryIn : inLinks.entrySet()) {
    		        		baseIDs.add(entryIn.getKey());
    		        	}
    				}
        		}
        	}
    	}
        

    }
    
    /* --------------------------------------------- */

    /**
     * Constructs the HITSRanker object
     * 
     * A set of linked documents can be presented as a graph.
     * Each page is a node in graph with a distinct nodeID associated with it.
     * There is an edge between two nodes if there is a link between two pages.
     * 
     * Each line in the links file has the following format:
     *  nodeID;outNodeID1,outNodeID2,...,outNodeIDK
     * This means that there are edges between nodeID and outNodeIDi, where i is between 1 and K.
     * 
     * Each line in the titles file has the following format:
     *  nodeID;pageTitle
     *  
     * NOTE: nodeIDs are consistent between these two files, but they are NOT the same
     *       as docIDs used by search engine's Indexer
     *
     * @param      linksFilename   File containing the links of the graph
     * @param      titlesFilename  File containing the mapping between nodeIDs and pages titles
     * @param      index           The inverted index
     */
    public HITSRanker( String linksFilename, String titlesFilename, Index index ) {
        this.index = index;
        this.PR = new PageRank();
        readDocs( linksFilename, titlesFilename );
    }
    
    /**
     * Constructs the HITSRanker object
     * 
     *
     * @param      engine   If this is false, all documents are ranked and the result are written to file
     */
    public HITSRanker( String linksFilename, String titlesFilename, Index index, boolean engine ) {
        this.index = index;
        this.PR = new PageRank();
        readDocs( linksFilename, titlesFilename );
        if (!engine) {
          rank();
        }
    }


    /* --------------------------------------------- */

    /**
     * A utility function that gets a file name given its path.
     * For example, given the path "davisWiki/hello.f",
     * the function will return "hello.f".
     *
     * @param      path  The file path
     *
     * @return     The file name.
     */
    public static String getFileName( String path ) {
        String result = "";
        StringTokenizer tok = new StringTokenizer( path, "\\/" );
        while ( tok.hasMoreTokens() ) {
            result = tok.nextToken();
        }
        return result;
    }


    /**
     * Reads the files describing the graph of the given set of pages.
     *
     * @param      linksFilename   File containing the links of the graph
     * @param      titlesFilename  File containing the mapping between nodeIDs and pages titles
     */
    void readDocs( String linksFilename, String titlesFilename) {
        int noDocs = PR.readDocs(linksFilename);
        createdReversedLinks(PR.link);
	    try {
    	    BufferedReader in = new BufferedReader( new FileReader( titlesFilename ));
    	    String line;
			while ((line = in.readLine()) != null) {    		
				String[] splittedLine = line.split(";");
				titleToId.put(splittedLine[1], Integer.parseInt(splittedLine[0]));
				IDToTitle.put(Integer.parseInt(splittedLine[0]),splittedLine[1]);
			}
		} catch (NumberFormatException | IOException e) {
			e.printStackTrace();
		}
    }

    /**
     * Creates a hashmap containing the inlinks for each document, similar to PageRank.links
     *
     * @param      link   Hashmap containing the outlinks
     */
    private void createdReversedLinks(HashMap<Integer, HashMap<Integer, Boolean>> link) {
    	for (Map.Entry<Integer, HashMap<Integer, Boolean>> entry : link.entrySet()) {
    	    int from = entry.getKey();
    	    HashMap<Integer, Boolean> toSet = entry.getValue();
        	for (Map.Entry<Integer,Boolean> entry2 : toSet.entrySet()) {
        	    int to = entry2.getKey();
        	    HashMap<Integer, Boolean> reversedEntry = reversedLink.get(to);
        	    if (reversedEntry==null) {
        	    	reversedLink.put(to, new HashMap<Integer,Boolean>());
        	    	reversedEntry = reversedLink.get(to);
        	    }
    	    	reversedEntry.put(from, true);
        	} 
    	} 
		
	}

	/**
     * Perform HITS iterations until convergence
     *
     * @param      titles  The titles of the documents in the root set
     */
    private void iterate(String[] titles) {
    	HITSSet hitsSet = new HITSSet(titles);
    	iterate(titles,hitsSet.baseIDs, hitsSet.rootIDs);
    }
    
	/**
     * Perform HITS iterations until convergence
     *
     * @param      titles  The titles of the documents in the root set
     * @param      baseIDs  Set of docIDs in the base set
     * @param      rootIDs  Set of docIDs in the root set
     */
    private void iterate(String[] titles, Set<Integer> baseIDs, Set<Integer> rootIDs) {
    	HashMap<Integer, Double> aOld = new HashMap<Integer,Double>();
    	HashMap<Integer, Double> hOld = new HashMap<Integer,Double>();
    	HashMap<Integer, Double> aNew = new HashMap<Integer,Double>();
    	HashMap<Integer, Double> hNew = new HashMap<Integer,Double>();
    	for (int docID: baseIDs) {

    		aOld.put(docID,1.0);
    		hOld.put(docID,1.0);
    	}
    	int convergedCount = 10;
    	while (convergedCount!=0) {
    		aNew = sparseMatrixVectorMul(hOld,reversedLink);    		
    		hNew = sparseMatrixVectorMul(aOld,PR.link);
    		if(!diff(aNew, aOld)&&!diff(hNew,hOld)) {
    			convergedCount--;
    		}
    		else {
    			convergedCount = 10;
    		}
    		aOld = (HashMap<Integer, Double>) aNew.clone();
    		hOld = (HashMap<Integer, Double>) hNew.clone();
    	}
    	authorities = new HashMap<Integer,Double>();
    	hubs = new HashMap<Integer,Double>();

    	for (Integer baseID : baseIDs) {
    		Double scoreA = 0.0;
    		Double scoreH = 0.0;
    		if(aNew.get(baseID)!= null) {
    			scoreA = aNew.get(baseID);
    		}
    		if(hNew.get(baseID)!= null) {
    			scoreH = hNew.get(baseID);
    		}
    		if(PR.docName[baseID]!= null) {
        		authorities.put(Integer.parseInt(PR.docName[baseID]),scoreA);
        		hubs.put(Integer.parseInt(PR.docName[baseID]),scoreH);
    		}
    		else {
    			System.out.println("PR.docName[baseID] null");
    		}

    	}
//    	System.out.println(hubs.size());
//    	System.out.println("rootIDs.size()" + rootIDs.size());
//    	System.out.println("baseIDs.size()" + baseIDs.size());
    }    

    
	/**
     * Checks if the elements in the given hashmap elements differ more than EPSILON 
     *
     * @param      vec1  The first hashmap
     * @param      vec2  The second hashmap
     * 
     * @return true if the elements differ, false otherwise
     */
    private boolean diff(HashMap<Integer, Double> vec1, HashMap<Integer, Double> vec2) {
    	for (Entry<Integer, Double> e : vec1.entrySet()) {
    		if (Math.abs(vec1.get(e.getKey())-vec2.get(e.getKey()))>EPSILON) {
    			return true;
    		}
    	}
    	return false;
	}

	/**
     * Performs sparse matrix vector product
     *
     * @param      base  The vector to be multiplied
     * @param      links  The matrix to be multiplied with
     * 
     * @return The resulting vector as a hashmap
     */
	HashMap<Integer, Double> sparseMatrixVectorMul(HashMap<Integer,Double> base,HashMap<Integer,HashMap<Integer,Boolean>>links) {
		HashMap<Integer,Double> result = new HashMap<Integer,Double>();
		for (Entry<Integer, Double> e : base.entrySet()) {
    		HashMap<Integer, Boolean> entries = links.get(e.getKey());
    		double entryVal = 0;
    		if (entries != null) {
            	for (Map.Entry<Integer,Boolean> e2 : entries.entrySet()) {
            		Double baseVal = base.get(e2.getKey());
            		if (baseVal!=null) {
            			entryVal+=baseVal;
            		}
            	}
    		}
    		if (base.containsKey(e.getKey())) {
        		result.put(e.getKey(), entryVal);
    		}
    	}	
		result = normalize(result);
		return result;
    }

	/**
     * Normalizes the given vector with euclidean norm
     *
     * @param      vec  The vector to be normalized as a hashmap
     * 
     * @return The normalized vector
     */
    private HashMap<Integer, Double> normalize(HashMap<Integer, Double> vec) {
    	double norm = norm(vec);
    	for (Entry<Integer, Double> e : vec.entrySet()) {
    		vec.put(e.getKey(), e.getValue()/norm);
    	}
    	return vec;
	}
    
	/**
     * Calculates the euclidean norm of the given vector
     *
     * @param      vec  The vector
     * 
     * @return The norm
     */
    private double norm(HashMap<Integer, Double> vec) {
    	double norm = 0;
    	for (Entry<Integer, Double> e : vec.entrySet()) {
    		norm+=Math.pow(e.getValue(),2);
    	}
    	return Math.sqrt(norm);
    }


	/**
     * Rank the documents in the subgraph induced by the documents present
     * in the postings list `post`.
     *
     * @param      post  The list of postings fulfilling a certain information need
     *
     * @return     A list of postings ranked according to the hub and authority scores.
     */
    PostingsList rank(PostingsList post) {
        LinkedList<PostingsEntry> entryList = post.getList();
        String[] titles = new String[entryList.size()];
        int i =0;
        for (PostingsEntry e : entryList) {
        	titles[i++]= getFileName(index.docNames.get(e.docID));
        }
        
    	HITSSet hitsSet = new HITSSet(titles);
    	iterate(titles,hitsSet.baseIDs, hitsSet.rootIDs);
        HashMap<Integer,Double> scores = new HashMap<Integer,Double>(hubs);
        double w1 = 0.5;
        double w2 = 0.5;
        PostingsList result = new PostingsList();
     	for (Entry<Integer, Double> e : authorities.entrySet()) {
     		Double sortedScoresVal = scores.get(e.getKey());	
     		scores.put(e.getKey(), w1*e.getValue()+w2*sortedScoresVal);
     	}
    	// rootIDs or baseIDs to use?
    	for (Integer linkRootID: hitsSet.rootIDs) {
    		Integer fileNameID = Integer.parseInt(PR.docName[linkRootID]);
    		String title = HITSRanker.getFileName(IDToTitle.get(fileNameID));
    		Integer realID = index.docIDs.get(title);
    		if (realID != null) {
        		Double combinedScore = scores.get(fileNameID);	
	        	PostingsEntry pEntry = new PostingsEntry();
            	pEntry.docID = realID;
                pEntry.score = combinedScore;
            	result.append(pEntry);
    		}
    	}
//    	System.out.println("hitsSet.baseIDs.size()" + hitsSet.baseIDs.size());
//    	System.out.println("hitsSet.rootIDs.size()" + hitsSet.rootIDs.size());
//
//    	System.out.println("hubs.size()" + hubs.size());
//    	System.out.println("authorities.size()" + authorities.size());
//
//    	System.out.println("result.size()" + result.size());
        result.sortList();
		return result;
    }


    /**
     * Sort a hash map by values in the descending order
     *
     * @param      map    A hash map to sorted
     *
     * @return     A hash map sorted by values
     */
    private HashMap<Integer,Double> sortHashMapByValue(HashMap<Integer,Double> map) {
        if (map == null) {
            return null;
        } else {
            List<Map.Entry<Integer,Double> > list = new ArrayList<Map.Entry<Integer,Double> >(map.entrySet());
            Collections.sort(list, new Comparator<Map.Entry<Integer,Double>>() {
                public int compare(Map.Entry<Integer,Double> o1, Map.Entry<Integer,Double> o2) { 
                    return (o2.getValue()).compareTo(o1.getValue()); 
                } 
            }); 
            HashMap<Integer,Double> res = new LinkedHashMap<Integer,Double>(); 
            for (Map.Entry<Integer,Double> el : list) { 
                res.put(el.getKey(), el.getValue()); 
            }
            return res;
        }
    } 


    /**
     * Write the first `k` entries of a hash map `map` to the file `fname`.
     *
     * @param      map        A hash map
     * @param      fname      The filename
     * @param      k          A number of entries to write
     */
    static void writeToFile(HashMap<Integer,Double> map, String fname, int k) {
        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter(fname));
            
            if (map != null) {
                int i = 0;
                for (Map.Entry<Integer,Double> e : map.entrySet()) {
                    i++;
                    writer.write(e.getKey() + ": " + String.format("%.5g%n", e.getValue()));
                    if (i >= k) break;
                }
            }
            writer.close();
        } catch (IOException e) {}
    }


    /**
     * Rank all the documents in the links file. Produces two files:
     *  hubs_top_30.txt with documents containing top 30 hub scores
     *  authorities_top_30.txt with documents containing top 30 authority scores
     */
    void rank() {
        iterate(titleToId.keySet().toArray(new String[0]));
        HashMap<Integer,Double> sortedHubs = sortHashMapByValue(hubs);
        HashMap<Integer,Double> sortedAuthorities = sortHashMapByValue(authorities);
        writeToFile(sortedHubs, "hubs_top_30.txt", 30);
        writeToFile(sortedAuthorities, "authorities_top_30.txt", 30);;
    }


    /* --------------------------------------------- */


    public static void main( String[] args ) {
        if ( args.length != 2 ) {
            System.err.println( "Please give the names of the link and title files" );
        }
        else {
            HITSRanker hr = new HITSRanker( args[0], args[1], null, false );
            hr.rank();
        }
    }
} 