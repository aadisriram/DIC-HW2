package storm.starter.trident.project.filters;

import storm.trident.operation.BaseFilter;
import storm.trident.tuple.TridentTuple;

/**
 * Only used to debug the code. Not used in production.
 *
 * Created by Sunil Kalmadka on 4/5/2015.
 */

public class PrintFilter  extends BaseFilter {

    String name;

	public PrintFilter() {

	}

	public PrintFilter(String name) {
		this.name = name;
	}

    @Override
    public boolean isKeep(TridentTuple tridentTuple) {
        System.out.println("PrintFilter ----  " +  name + " : " + tridentTuple);
        return true;
    }
}