package ir;
import java.util.Comparator;

	 
	public class DocIdSorter implements Comparator<PostingsEntry> {
	    @Override
	    public int compare(PostingsEntry e1, PostingsEntry e2) {
	        
	    	if(e1.docID>e2.docID)
	    		return 1;
	    	else if (e1.docID<e2.docID)
	    		return -1;
	    	else
	    		return 0;
	    }
	}