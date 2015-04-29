package storm.starter.trident.project.countmin.state;

import org.apache.avro.generic.GenericData;
import storm.starter.trident.project.functions.Tweet;
import storm.trident.state.State;

import java.io.Serializable;
import java.util.*;

/**
 * Created by parth on 4/25/15.
 */
public class InvertedIndexState implements State, Serializable {
    private int windowSize = 1000;

    Queue<TweetWord> slidingWindow = new LinkedList<TweetWord>();
    Map<String, Set<TweetWord>> invertedIndex = new HashMap<String, Set<TweetWord>>();

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
                    Set<TweetWord> ids = invertedIndex.remove(hashtag);
                    Set<TweetWord> newSet = new HashSet<TweetWord>();
                    for(TweetWord tweet : ids) {
                        if(tweet.getId() != oldId) {
                            //System.out.println("DEBUG " + oldId + ":" + tweet.getId());
                            newSet.add(tweet);
                        }
                    }
                    if(!newSet.isEmpty()) {
                        invertedIndex.put(hashtag, newSet);
                    }
                }
            }
        }

        slidingWindow.add(newTweet);
        long id = newTweet.getId();
        List<String> hashtags = newTweet.getHashtags();
        String username = newTweet.getUsername();
        for(String hashtag : hashtags) {
            if(invertedIndex.containsKey(hashtag)) {
                Set<TweetWord> set = invertedIndex.remove(hashtag);
                set.add(new TweetWord(new ArrayList<String>(), id, username));
                invertedIndex.put(hashtag, set);
            } else {
                Set<TweetWord> idSet = new HashSet<TweetWord>();
                idSet.add(new TweetWord(new ArrayList<String>(), id, username));
                invertedIndex.put(hashtag, idSet);
            }
        }
    }

    public Set<TweetWord> getTweetIds(String hashtag) {
        //System.out.println(invertedIndex.keySet().size());
        if(invertedIndex.containsKey(hashtag)) {
            //System.out.println("DEBUG window : " + slidingWindow.size());
            //System.out.println("DEBUG index : " + invertedIndex.size());
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
