package storm.starter.trident.project.functions;

import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import twitter4j.HashtagEntity;
import twitter4j.Status;
import twitter4j.TwitterObjectFactory;
import twitter4j.User;

import java.util.HashSet;
import java.util.Set;

// Local functions

/**
 * @author Aaditya Sriram
 */
public class ParseTweet extends BaseFunction {
    private static final Logger log = LoggerFactory.getLogger(ParseTweet.class);

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        Status parsed = parse((String)tuple.get(0));
//        Status parsed = (Status)tuple.get(0);
        User user = parsed.getUser();
        String userScreenName = user.getScreenName();

        StringBuilder tweetText = new StringBuilder();
        HashtagEntity[] hashtagEntities = parsed.getHashtagEntities();

        Set<String> hashtags = new HashSet<String>();

        if(hashtagEntities != null) {
            for(HashtagEntity entity : hashtagEntities) {
                if(entity.getText() != null) {
                    String hashtag = entity.getText();
                    hashtag = hashtag.replaceAll("[^a-zA-Z0-9]", "");
                    hashtag = hashtag.toLowerCase();
                    if(!hashtag.isEmpty()) {
                        hashtags.add(hashtag);
                    }
                }
            }

            for(String hashtag : hashtags) {
                tweetText.append(hashtag + " ");
            }
        }
        collector.emit(new Values(tweetText.toString(), parsed.getId(), userScreenName));
    }

    private Status parse(String rawJson) {
        try {
            Status parsed = TwitterObjectFactory.createStatus(rawJson);
            return parsed;
        } catch (Exception e) {
            e.printStackTrace();
            log.warn("Invalid tweet json -> " + rawJson, e);
            return null;
        }
    }
}
