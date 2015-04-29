package storm.starter.trident.project.filters;

import storm.trident.operation.BaseFilter;
import storm.trident.tuple.TridentTuple;

/**
 * Created by parthsatra on 4/28/15.
 */
public class EmptyStringFilter extends BaseFilter {

    @Override
    public boolean isKeep(TridentTuple tridentTuple) {
        String value = tridentTuple.getString(0);
        if(value == null || value.trim().isEmpty()) {
            return false;
        }
        return true;
    }
}
