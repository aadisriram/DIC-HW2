package storm.starter.trident.project.countmin.state;

import backtype.storm.tuple.Values;
import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseQueryFunction;
import storm.trident.state.State;
import storm.trident.tuple.TridentTuple;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by parth on 4/25/15.
 */
public class InvertedIndexQuery extends BaseQueryFunction<InvertedIndexState, String> {

    @Override
    public List<String> batchRetrieve(InvertedIndexState invertedIndexState, List<TridentTuple> input) {
        List<String> ids = new ArrayList<String>();
        Set<Long> uniqueIds = new HashSet<Long>();
        for(TridentTuple tuple : input) {
            Set<Long> set = invertedIndexState.getTweetIds(tuple.getString(0));
            uniqueIds.addAll(set);
        }

        StringBuilder sb = new StringBuilder();
        sb.append(",");

        for(long id : uniqueIds) {
            sb.append(id+",");
        }
        ids.add(sb.toString());
        return ids;
    }

    @Override
    public void execute(TridentTuple tridentTuple, String s, TridentCollector tridentCollector) {
        tridentCollector.emit(new Values(s));
    }
}
