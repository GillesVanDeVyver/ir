package ir;

import java.util.*;
import java.io.*;

public class PageRank {

    /**  
     *   Maximal number of documents. We're assuming here that we
     *   don't have more docs than we can keep in main memory.
     */
    final static int MAX_NUMBER_OF_DOCS = 2000000;

    /**
     *   Mapping from document names to document numbers.
     */
    HashMap<String,Integer> docNumber = new HashMap<String,Integer>();

    /**
     *   Mapping from document numbers to document names
     */
    String[] docName = new String[MAX_NUMBER_OF_DOCS];

    /**  
     *   A memory-efficient representation of the transition matrix.
     *   The outlinks are represented as a HashMap, whose keys are 
     *   the numbers of the documents linked from.<p>
     *
     *   The value corresponding to key i is a HashMap whose keys are 
     *   all the numbers of documents j that i links to.<p>
     *
     *   If there are no outlinks from i, then the value corresponding 
     *   key i is null.
     */
    HashMap<Integer,HashMap<Integer,Boolean>> link = new HashMap<Integer,HashMap<Integer,Boolean>>();

    /**
     *   The number of outlinks from each node.
     */
    int[] out = new int[MAX_NUMBER_OF_DOCS];

    /**
     *   The probability that the surfer will be bored, stop
     *   following links, and take a random jump somewhere.
     */
    final static double BORED = 0.15;
    
    final static double c = 0.85;
    
    final static int[] EXACTTOP30 = {121,21,245,1531,1367,31,80,1040,254,452,
    								157,392,169,100,561,3870,997,884,202,8,
    								72,145,27,645,490,2883,81,942,125,247};

    
    /**
     *   Convergence criterion: Transition probabilities do not 
     *   change more that EPSILON from one iteration to another.
     */
    final static double EPSILON = 0.0001;

	private static final int MAXT = 50;

       
    /* --------------------------------------------- */


    public PageRank( String filename ) {
		int noOfDocs = readDocs( filename );
//		int[] seq = {1,10,100,1000};
//		int nbIter = 10;
//		for (int i = 0; i < seq.length; i++) {
//			double avg = 0;
//			int m = seq[i];
//			int N = noOfDocs*m;
//			for (int j =0; j<nbIter; j++) {
//				avg += iterateMC5( noOfDocs, N, c);
//			}
//			avg = avg/nbIter;
//			System.out.println(avg);
//		}
//		iterateMC4UntilConvergence(noOfDocs, c);
		iterate(noOfDocs,10000);
    }
    
    public PageRank() {
	}


    /* --------------------------------------------- */





	/**
     *   Reads the documents and fills the data structures. 
     *
     *   @return the number of documents read.
     */
    int readDocs( String filename ) {
	int fileIndex = 0;
	try {
	    System.err.print( "Reading file... " );
	    BufferedReader in = new BufferedReader( new FileReader( filename ));
	    String line;
	    while ((line = in.readLine()) != null && fileIndex<MAX_NUMBER_OF_DOCS ) {
		int index = line.indexOf( ";" );
		String title = line.substring( 0, index );
		Integer fromdoc = docNumber.get( title );
		//  Have we seen this document before?
		if ( fromdoc == null ) {	
		    // This is a previously unseen doc, so add it to the table.
		    fromdoc = fileIndex++;
		    docNumber.put( title, fromdoc );
		    docName[fromdoc] = title;
		}
		// Check all outlinks.
		StringTokenizer tok = new StringTokenizer( line.substring(index+1), "," );
		while ( tok.hasMoreTokens() && fileIndex<MAX_NUMBER_OF_DOCS ) {
		    String otherTitle = tok.nextToken();
		    Integer otherDoc = docNumber.get( otherTitle );
		    if ( otherDoc == null ) {
			// This is a previousy unseen doc, so add it to the table.
			otherDoc = fileIndex++;
			docNumber.put( otherTitle, otherDoc );
			docName[otherDoc] = otherTitle;
		    }
		    // Set the probability to 0 for now, to indicate that there is
		    // a link from fromdoc to otherDoc.
		    if ( link.get(fromdoc) == null ) {
			link.put(fromdoc, new HashMap<Integer,Boolean>());
		    }
		    if ( link.get(fromdoc).get(otherDoc) == null ) {
			link.get(fromdoc).put( otherDoc, true );
			out[fromdoc]++;
		    }
		}
	    }
	    if ( fileIndex >= MAX_NUMBER_OF_DOCS ) {
		System.err.print( "stopped reading since documents table is full. " );
	    }
	    else {
		System.err.print( "done. " );
	    }
	}
	catch ( FileNotFoundException e ) {
	    System.err.println( "File " + filename + " not found!" );
	}
	catch ( IOException e ) {
	    System.err.println( "Error reading file " + filename );
	}
	System.err.println( "Read " + fileIndex + " number of documents" );
	return fileIndex;
    }


    /* --------------------------------------------- */


    /*
     *   Chooses a probability vector a, and repeatedly computes
     *   aP, aP^2, aP^3... until aP^i = aP^(i+1).
     *   prints 30 highest ranked pages
     */
    void iterate( int numberOfDocs, int maxIterations ) {
	    double[] aOld = new double[numberOfDocs];
	    double[] aNew = new double[numberOfDocs];
	    Arrays.fill(aOld, 1/((double) numberOfDocs));
	    double error = EPSILON+1;
	    int iter = 0;
	    while (error>EPSILON && iter<maxIterations) {
	    	Arrays.fill(aNew, 0);
    		for(int i = 0; i < aNew.length; i++){ // for every entry in aNew
    			for(int j = 0; j < aOld.length; j++){ // for every entry in aOld
		    		double factor = 0L;
		    		HashMap<Integer,Boolean> outMap = link.get(j);
		    		if (outMap!= null) {
		    			factor = BORED*(1/((double) numberOfDocs));
			    		if (outMap.get(i) != null) {
			    			factor += (1-BORED)*1/((double) outMap.size());
			    		}
		    		}
		    		else {
		    			factor= (1/((double)numberOfDocs));
		    		}
		    		aNew[i]+=factor*aOld[j];
		    	}
	    	}
	    	iter++;
	    	error = norm(substract(aOld, aNew));
	    	aOld = aNew.clone();
	    }
    	
	    printTop30(aNew);


    }
    

    
    double iterateMC1( int numberOfDocs, int N, double c ) {
    	double[] pi = new double[numberOfDocs];
    	Arrays.fill(pi, 0);
    	
    	for (int i =0; i<N; i++) {
    		int startID = (int) (Math.random()*numberOfDocs);
    		int terminalNode = randomWalk(startID, c, numberOfDocs);
    		pi[terminalNode]++;
    	}
    	for(int i = 0; i < numberOfDocs; i++)
    	{
    	  pi[i] = pi[i]/N;
    	}
//    	printTop30(pi);
    	return goodness(pi);
    }
    
    double iterateMC5( int numberOfDocs, int N, double c ) {
    	double[] pi = new double[numberOfDocs];
    	Arrays.fill(pi, 0);
    	int totalNbVisits = 0;
    	for (int i =0; i<N; i++) {
    		int startID = (int) (Math.random()*numberOfDocs);
    		totalNbVisits = randomWalkSeq(startID, c, numberOfDocs,pi,totalNbVisits );
    	}
    	for(int i = 0; i < numberOfDocs; i++)
    	{
    	  pi[i] = pi[i]/(totalNbVisits);
    	}
//    	printTop30(pi);
    	return goodness(pi);
    }
  

    double iterateMC2( int numberOfDocs, int m, double c) {
    	double[] pi = new double[numberOfDocs];
    	Arrays.fill(pi, 0);
    	for (int j =0; j<m; j++) {
	    	for (int i =0; i<numberOfDocs; i++) {
	    		int startID = i;
	    		int terminalNode = randomWalk(startID, c, numberOfDocs);
	    		pi[terminalNode]++;	
	    	}
    	}
    	for(int i = 0; i < numberOfDocs; i++)
    	{
    	  pi[i] = pi[i]/(numberOfDocs*m);
    	}
//    	printTop30(pi);
    	return goodness(pi);
    }
	
    double iterateMC4( int numberOfDocs, int m, double c) {
    	double[] pi = new double[numberOfDocs];
    	Arrays.fill(pi, 0);
    	int totalNbVisits = 0;
    	for (int j =0; j<m; j++) {
	    	for (int i =0; i<numberOfDocs; i++) {
	    		int startID = i;
	    		totalNbVisits = randomWalkSeq(startID, c, numberOfDocs,pi,totalNbVisits );
	    	}
    	}
    	for(int i = 0; i < numberOfDocs; i++)
    	{
    	  pi[i] = pi[i]/(totalNbVisits);
    	}
//    	printTop30(pi);
    	return goodness(pi);
    }
    
    void iterateMC4UntilConvergence( int numberOfDocs, double c) {
    	double[] oldPi = new double[numberOfDocs];
    	Arrays.fill(oldPi, 0);
    	double[] newPi = new double[numberOfDocs];
    	Arrays.fill(newPi, 0);
    	int totalNbVisits = 0;
    	int maxIter = 10000000;
    	int iter = 0;
    	int convergedCounter = 10;
		while (convergedCounter  != 0 && iter<maxIter) {
	    	for (int i =0; i<numberOfDocs; i++) {
	    		int startID = i;
	    		totalNbVisits = randomWalkSeq(startID, c, numberOfDocs,newPi,totalNbVisits );
	    	}
	    	if (equalTop30(oldPi,newPi)) {
	    		convergedCounter--;
	    		System.out.println("convergedCounter" + convergedCounter);
	    	}else {
	    		convergedCounter = 10;
	    	}
	    	oldPi = newPi.clone();
    	}

		for(int i = 0; i < numberOfDocs; i++)
		{
		  newPi[i] = newPi[i]/(totalNbVisits);
		}
		printTop30(newPi);

    }
	
	private boolean equalTop30(double[] pi1, double[] pi2) {
    	Integer[] sortedInd1 = argsort(pi1, false);
    	Integer[] sortedInd2 = argsort(pi2, false);
    	int count = 0;
    	for(int i = 0; i <30; i++){
    		if((int) sortedInd1[i] != (int) sortedInd2[i]) {
    			System.out.println(count);
    			return false;
    		}
    		else {
    			count++;
    		}
    	}
    	System.out.println("converged " + count);
		return true;
	}


	void printTop30(double[] pi ) {
    	Integer[] sortedInd = argsort(pi, false);
    	for(int i = 0; i <30; i++){
    		System.out.println("docID: " + docName[sortedInd[i]]+ " score: " +pi[sortedInd[i]]);
    	}
	}
    
	double goodness(double[] pi ) {
		double result = 0;
    	Integer[] rarnkVector = argsort(pi, false);
    	for (int i =0; i<rarnkVector.length; i++) {
    		rarnkVector[i] = Integer.parseInt(docName[rarnkVector[i]]);
    	}
    	for(int i = 0; i <30; i++){
    		int docID = EXACTTOP30[i];
    		if(rarnkVector[i] != docID) {
    			int pos = Arrays.asList(rarnkVector).indexOf(docID);
    			result += Math.pow((pos-i),2);
    		}
    	}
    	return result;
	}

    
    private void writeToFile(String fileName, double[] pi, int numberOfDocs) {
	    RandomAccessFile outputFile ;
        try {
        	outputFile = new RandomAccessFile( fileName, "rw" );
    		outputFile.seek(0);
        	Integer[] sortedInd = argsort(pi, false);
        	outputFile.writeChars(numberOfDocs + ":");
        	for(int i = 0; i <numberOfDocs; i++){
            	outputFile.writeChars(docName[sortedInd[i]] + ",");
            	outputFile.writeChars(pi[sortedInd[i]] + ";");
        	}
        	
        } catch ( IOException e ) {
            e.printStackTrace();
        }


    }


    private int randomWalk(int startID, double c, int numberOfDocs) {
    	int currentNode = startID;
    	int t = 0;
    	while (t<MAXT) {
        	HashMap<Integer,Boolean> outMap = link.get(currentNode);
        	if (outMap!=null) {
        		Object[] outIds = (Object[]) outMap.keySet().toArray();
        		currentNode = (int) outIds[new Random().nextInt(outIds.length)];
        	}
        	else { //sink => jump randomly
        		currentNode = (int) (Math.random()* numberOfDocs);
        	}
        	double r = Math.random();
        	if (r>c)
        		return currentNode;
        	t++;
    	}
		return currentNode;
	}
    
    private int randomWalkSeq(int startID, double c, int numberOfDocs, double[] pi, int totalNbVisits) {
    	int currentNode = startID;
    	int t = 0;
    	while (t<MAXT) {
    		totalNbVisits++;
    		pi[currentNode]++;
        	HashMap<Integer,Boolean> outMap = link.get(currentNode);
        	if (outMap!=null) {
        		Object[] outIds = (Object[]) outMap.keySet().toArray();
        		currentNode = (int) outIds[new Random().nextInt(outIds.length)];
        	}
        	else { //sink => terminate
        		return totalNbVisits;
        	}
        	double r = Math.random();
        	if (r>c)
        		return totalNbVisits;
        	t++;
    	}
		return totalNbVisits;
	}


	double[] substract (double[] vec1, double[] vec2) {
    	double[] result = new double[vec1.length];
    	for(int i = 0; i < vec1.length; i++) {
    		result[i] = vec1[i]-vec2[i];
    	}
    	return result;
    }
    
    double norm (double[] vec) {
    	double result = 0;
    	for(int i = 0; i < vec.length; i++) {
    		result+=Math.abs((vec[i]));
    	}
    	return ((result));
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
    
    
    /* --------------------------------------------- */


    public static void main( String[] args ) {
	if ( args.length != 1 ) {
	    System.err.println( "Please give the name of the link file" );
	}
	else {
	    new PageRank( args[0] );
	}
    }
}
