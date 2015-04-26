package storm.starter.trident.project.countmin.state;

import backtype.storm.task.IMetricsContext;
import storm.trident.state.State;
import storm.trident.state.StateFactory;

import java.util.Map;

/**
 * Created by parth on 4/25/15.
 */
public class InvertedIndexStateFactory implements StateFactory {

    protected int windowsize;

    public InvertedIndexStateFactory(int windowsize) {
        this.windowsize = windowsize;
    }

    @Override
    public State makeState(Map map, IMetricsContext iMetricsContext, int i, int i1) {
        return new InvertedIndexState(windowsize);
    }
}
