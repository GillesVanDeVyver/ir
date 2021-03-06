/*  
 *   This file is part of the computer assignment for the
 *   Information Retrieval course at KTH.
 * 
 *   Johan Boye, 2017
 */  

package ir;

import java.io.*;
import java.util.*;
import java.util.Map.Entry;
import java.nio.charset.*;


/**
 *   Processes a directory structure and indexes all PDF and text files.
 */
public class Indexer {

    /** The index to be built up by this Indexer. */
    Index index;

    /** K-gram index to be built up by this Indexer */
    KGramIndex kgIndex;

    /** The next docID to be generated. */
    int lastDocID = 0;

    /** The patterns matching non-standard words (e-mail addresses, etc.) */
    String patterns_file;


    /* ----------------------------------------------- */


    /** Constructor */
    public Indexer( Index index, KGramIndex kgIndex, String patterns_file ) {
        this.index = index;
        this.kgIndex = kgIndex;
        this.patterns_file = patterns_file;
    }


    /** Generates a new document identifier as an integer. */
    private int generateDocID() {
        return lastDocID++;
    }



    /**
     *  Tokenizes and indexes the file @code{f}. If <code>f</code> is a directory,
     *  all its files and subdirectories are recursively processed.
     */
    public void processFiles( File f, boolean is_indexing ) {
        // do not try to index fs that cannot be read
        if (is_indexing) {
            if ( f.canRead() ) {
                if ( f.isDirectory() ) {
                    String[] fs = f.list();
                    // an IO error could occur
                    if ( fs != null ) {
                        for ( int i=0; i<fs.length; i++ ) {
                            processFiles( new File( f, fs[i] ), is_indexing );
                        }
                    }
                } else {
                    // First register the document and get a docID
                    int docID = generateDocID();
                    if ( docID%1000 == 0 ) System.err.println( "Indexed " + docID + " files" );
                    try {
                        Reader reader = new InputStreamReader( new FileInputStream(f), StandardCharsets.UTF_8 );
                        Tokenizer tok = new Tokenizer( reader, true, false, true, patterns_file );
                        int offset = 0;
                        while ( tok.hasMoreTokens() ) {
                            String token = tok.nextToken();
                            insertIntoIndex( docID, token, offset++ );
                        }
                        index.docNames.put( docID, f.getPath() );
                        
                        String docName = HITSRanker.getFileName(f.getPath());
                        index.docIDs.put(HITSRanker.getFileName(f.getPath()), docID );
                        index.docLengths.put( docID, offset );
                        reader.close();
                    } catch ( IOException e ) {
                        System.err.println( "Warning: IOException during indexing." );
                    }
                }
            }
        }
    }


    /* ----------------------------------------------- */


    /**
     *  Indexes one token.
     */
    public void insertIntoIndex( int docID, String token, int offset ) {
        index.insert( token, docID, offset );
        if (kgIndex != null)
            kgIndex.insert(token);
    }


	public void processFilesSecondPass(File f) {
		
        if ( f.canRead() ) {
            if ( f.isDirectory() ) {
                String[] fs = f.list();
                // an IO error could occur
                if ( fs != null ) {
                    for ( int i=0; i<fs.length; i++ ) {
                    	processFilesSecondPass( new File( f, fs[i] ) );
                    }
                }
            } else {
            	
                // First register the document and get a docID
                int docID = generateDocID();
                if ( docID%1000 == 0 ) System.err.println( "Indexed " + docID + " files (second pass)" );
                try {
                    Reader reader = new InputStreamReader( new FileInputStream(f), StandardCharsets.UTF_8 );
                    Tokenizer tok = new Tokenizer( reader, true, false, true, patterns_file );
                    int offset = 0;
                    
                    float euclidLen = 0;
                    HashMap<String,Integer> countMap = new HashMap<String,Integer>();
                    while ( tok.hasMoreTokens() ) {
                        String token = tok.nextToken();
                        if (countMap.get(token)!= null) {
                        	countMap.put(token, countMap.get(token)+1);
                        }
                        else
                        	countMap.put(token,1);
                        
                    }
                    
                    
                    for (Entry<String, Integer> entry : countMap.entrySet()) {
                    	String token = entry.getKey();
                        int tf = entry.getValue();
                        
            			PostingsList pList = index.getPostings(token);
            	    	LinkedList<PostingsEntry> eList = pList.getList();
            	    	
            	    	int N = Index.docNames.size();
			    		float frac = ((float) N)/((float)eList.size());
			    		float idf = (float) Math.log(frac);
                        
                        
                        euclidLen += Math.pow(tf*idf, 2);
                    }
                    
                    
//                        System.out.println("f.getPath()"+f.getPath());
//                        System.out.println("Math.sqrt(euclidLen)"+Math.sqrt(euclidLen));
                    
                    index.euclidDocLengths.put( docID, (double) Math.sqrt(euclidLen));
                    
                    
                    
                    reader.close();
                } catch ( IOException e ) {
                    System.err.println( "Warning: IOException during indexing." );
                }
            }
        }
    }
		
}

