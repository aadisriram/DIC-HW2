package storm.starter.trident.project.countmin.state;

import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseStateUpdater;
import storm.trident.tuple.TridentTuple;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by parth on 4/25/15.
 */
public class InvertedIndexUpdater extends BaseStateUpdater<InvertedIndexState> {
    @Override
    public void updateState(InvertedIndexState invertedIndexState, List<TridentTuple> list, TridentCollector tridentCollector) {
        for(TridentTuple tuple : list) {
            // Gets all the space separated hashtags.
            String hashTags = tuple.getString(0);
            long id = tuple.getLong(1);
            String[] tag = hashTags.split(" ");
            // Creates the list to be added to the state
            List<String> tweetList = new ArrayList<String>();
            for(String t : tag) {
                if(t != null && t.trim().length() != 0) {
                    tweetList.add(t);
                }
            }
            // Adds the list to the state.
            invertedIndexState.add(new TweetWord(tweetList,id));
        }
    }
}
