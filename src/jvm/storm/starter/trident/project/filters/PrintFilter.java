package storm.starter.trident.project.filters;

import storm.trident.operation.BaseFilter;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import storm.trident.operation.TridentOperationContext;
import java.util.Map;

/**
 * Print filter for printing Trident tuples. 
 * used in debugging
 *
 * @author preems
 */

public class PrintFilter extends BaseFilter {

	String prefix="";

	public PrintFilter(String prefix) {
		this.prefix=prefix;
	}

	public PrintFilter() {

	}

	private int partitionIndex;

	@Override
  	public void prepare(Map conf, TridentOperationContext context) {
    	this.partitionIndex = context.getPartitionIndex();
  	}
  
  	@Override
  	public boolean isKeep(TridentTuple tuple) {
    	if(true) {
      		System.err.println("I am partition [" + partitionIndex + "]");
    	}
    	return true;
  	}
}