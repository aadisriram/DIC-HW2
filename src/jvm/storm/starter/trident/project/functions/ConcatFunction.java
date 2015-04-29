package storm.starter.trident.project.functions;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

/**
 * Created by parthsatra on 4/28/15.
 */
public class ConcatFunction extends BaseFunction {

    @Override
    public void execute(TridentTuple tridentTuple, TridentCollector tridentCollector) {
        long id = tridentTuple.getLong(0);
        String uname = tridentTuple.getString(1);
        StringBuilder sb = new StringBuilder();
        sb.append(id);
        sb.append("_");
        sb.append(uname);
        tridentCollector.emit(new Values(sb.toString()));
    }
}
