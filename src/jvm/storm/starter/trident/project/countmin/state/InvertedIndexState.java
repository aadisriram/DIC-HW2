package storm.starter.trident.project.countmin.state;

import storm.trident.state.State;

import java.io.Serializable;
import java.util.*;

/**
 * Created by parth on 4/25/15.
 */
public class InvertedIndexState implements State, Serializable {
    private int windowSize = 1000;

    Queue<TweetWord> slidingWindow = new LinkedList<TweetWord>();
    Map<String, Set<Long>> invertedIndex = new HashMap<String, Set<Long>>();

    public InvertedIndexState(int windowSize) {
        this.windowSize = windowSize;
    }

    public void add(TweetWord newTweet) {
        if(newTweet == null) {
            return;
        }
        if(slidingWindow.size() == windowSize) {
            TweetWord oldTweet = slidingWindow.poll();
            List<String> hashtags = oldTweet.getHashtags();
            long oldId = oldTweet.getId();
            for(String hashtag: hashtags) {
                if(invertedIndex.containsKey(hashtag)) {
                    Set<Long> ids = invertedIndex.remove(hashtag);
                    if(ids.contains(oldId)) {
                        ids.remove(oldId);
                    }
                    invertedIndex.put(hashtag, ids);
                }
            }
        }

        slidingWindow.add(newTweet);
        long id = newTweet.getId();
        List<String> hashtags = newTweet.getHashtags();
        for(String hashtag : hashtags) {
            if(invertedIndex.containsKey(hashtag)) {
                Set<Long> set = invertedIndex.remove(hashtag);
                set.add(id);
                invertedIndex.put(hashtag, set);
            } else {
                Set<Long> idSet = new HashSet<Long>();
                idSet.add(id);
                invertedIndex.put(hashtag, idSet);
            }
        }
    }

    public Set<Long> getTweetIds(String hashtag) {
        if(invertedIndex.containsKey(hashtag)) {
            return invertedIndex.get(hashtag);
        } else {
            return Collections.emptySet();
        }
    }

    @Override
    public void beginCommit(Long aLong) {

    }

    @Override
    public void commit(Long aLong) {

    }
}
