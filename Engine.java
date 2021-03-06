/*  
 *   This file is part of the computer assignment for the
 *   Information Retrieval course at KTH.
 * 
 *   Johan Boye, 2017
 */

package ir;

import java.util.ArrayList;
import java.util.Map.Entry;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;

/**
 *  This is the main class for the search engine.
 */
public class Engine {

    /** The inverted index. */
    //Index index = new HashedIndex();
//    Index index = new PersistentHashedIndex();
//    Index index = new PersistentScalableHashedIndex();
	Index index;

    /** The indexer creating the search index. */
    Indexer indexer;

    /** K-gram index */
    KGramIndex kgIndex;

    /** The searcher used to search the index. */
    Searcher searcher;

    /** Spell checker */
    SpellChecker speller;

    /** The engine GUI. */
    SearchGUI gui;

    /** Directories that should be indexed. */
    ArrayList<String> dirNames = new ArrayList<String>();

    /** Lock to prevent simultaneous access to the index. */
    Object indexLock = new Object();

    /** The patterns matching non-standard words (e-mail addresses, etc.) */
    String patterns_file = null;

    /** The file containing the logo. */
    String pic_file = "";

    /** The file containing the pageranks. */
    String rank_file = "";

    /** For persistent indexes, we might not need to do any indexing. */
    boolean is_indexing = true;

	private HITSRanker hitsRanker;


    /* ----------------------------------------------- */


    /**  
     *   Constructor. 
     *   Indexes all chosen directories and files
     */
    public Engine( String[] args ) {
        decodeArgs( args );
//        Index index = new PersistentScalableHashedIndex(is_indexing);
        Index index = new HashedIndex();
//      Index index = new PersistentHashedIndex();
        indexer = new Indexer( index, kgIndex, patterns_file );
        hitsRanker = new HITSRanker("linksDavis.txt", "davisTitles.txt", index );
        searcher = new Searcher( index, kgIndex, hitsRanker );
        gui = new SearchGUI( this );
        gui.init();
        /* 
         *   Calls the indexer to index the chosen directory structure.
         *   Access to the index is synchronized since we don't want to 
         *   search at the same time we're indexing new files (this might 
         *   corrupt the index).
         */
        if (is_indexing) {
            synchronized ( indexLock ) {
                gui.displayInfoText( "Indexing, please wait..." );
                long startTime = System.currentTimeMillis();
                for ( int i=0; i<dirNames.size(); i++ ) {
                    File dokDir = new File( dirNames.get( i ));
                    indexer.processFiles( dokDir, is_indexing );
                }
                long elapsedTime = System.currentTimeMillis() - startTime;
                gui.displayInfoText( String.format( "Indexing done in %.1f seconds.", elapsedTime/1000.0 ));
                index.cleanup();
            }
        } else {
            gui.displayInfoText( "Index is loaded from disk" );
        }
        
//      calculateDocLenghts("euclid.txt"); //only once
        readDocLengths("euclid.txt");
        searcher.initPageRankVector();
    }


    private void readDocLengths(String fileName) {
	    try {
    	    BufferedReader in = new BufferedReader( new FileReader( fileName ));
    	    String line;
			while ((line = in.readLine()) != null) {    		
				String[] splittedLine = line.split(":");
				index.euclidDocLengths.put(Integer.parseInt(splittedLine[0]), Double.parseDouble(splittedLine[1]));
			}
		} catch (NumberFormatException | IOException e) {
			e.printStackTrace();
		}		
	}


	private void calculateDocLenghts(String fileName) {
    	indexer.lastDocID=0;
        synchronized ( indexLock ) {
            for ( int i=0; i<dirNames.size(); i++ ) {
                File dokDir = new File( dirNames.get( i ));
                indexer.processFilesSecondPass( dokDir );
            }
        }
        HITSRanker.writeToFile(index.euclidDocLengths, fileName, index.euclidDocLengths.size());
        
	}


	/* ----------------------------------------------- */

    /**
     *   Decodes the command line arguments.
     */
    private void decodeArgs( String[] args ) {
        int i=0, j=0;
        while ( i < args.length ) {
            if ( "-d".equals( args[i] )) {
                i++;
                if ( i < args.length ) {
                    dirNames.add( args[i++] );
                }
            } else if ( "-p".equals( args[i] )) {
                i++;
                if ( i < args.length ) {
                    patterns_file = args[i++];
                }
            } else if ( "-l".equals( args[i] )) {
                i++;
                if ( i < args.length ) {
                    pic_file = args[i++];
                }
            } else if ( "-r".equals( args[i] )) {
                i++;
                if ( i < args.length ) {
                    rank_file = args[i++];
                }
            } else if ( "-ni".equals( args[i] )) {
                i++;
                is_indexing = false;
            } else {
            	is_indexing = false;
                break;
            }
        }                   
    }


    /* ----------------------------------------------- */


    public static void main( String[] args ) {
        Engine e = new Engine( args );
    }

}

