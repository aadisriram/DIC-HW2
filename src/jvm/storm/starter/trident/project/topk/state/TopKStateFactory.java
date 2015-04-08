package storm.starter.trident.project.topk.state;

import storm.trident.state.StateFactory;
import storm.trident.state.State;
import java.util.Map;
import backtype.storm.task.IMetricsContext;

public class TopKStateFactory implements StateFactory {

	protected int topk_size;

	public TopKStateFactory(int topk_size) {
		this.topk_size = topk_size;
	}

   @Override
   public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
      return new TopKSketchState(topk_size);
   } 
}