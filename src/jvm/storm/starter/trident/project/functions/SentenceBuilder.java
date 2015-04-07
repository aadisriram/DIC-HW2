package storm.starter.trident.project.functions;

import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import storm.starter.trident.project.functions.Tweet;
import com.google.common.collect.Lists;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.ReducerAggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.builtin.FilterNull;
import storm.trident.testing.FixedBatchSpout;
import storm.trident.tuple.TridentTuple;

import java.io.IOException;

public class SentenceBuilder extends BaseFunction {
    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        String sentence =  (String)tuple.getString(0);
        collector.emit(new Values( new String(sentence)));
    }
}

