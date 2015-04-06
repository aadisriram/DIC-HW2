package storm.starter.trident.project.countmin.state;

import storm.trident.state.BaseStateUpdater;
import storm.trident.tuple.TridentTuple;
import storm.trident.operation.TridentCollector;
import java.util.List;
import java.util.ArrayList;

public class CountMinSketchUpdater extends BaseStateUpdater<CountMinSketchState> {
    public void updateState(CountMinSketchState state, 
    						List<TridentTuple> tuples, TridentCollector collector) {
    	
        for(TridentTuple t: tuples) {
            state.add(t.getString(0), 1);
        }
    }
}
