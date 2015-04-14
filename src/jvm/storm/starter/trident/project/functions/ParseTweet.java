package storm.starter.trident.project.functions;

import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;
import twitter4j.User;
import twitter4j.json.DataObjectFactory;



// Local functions
import storm.starter.trident.project.functions.Content;
import storm.starter.trident.project.functions.ContentExtracter;

import twitter4j.HashtagEntity;

/**
 * @author Enno Shioji (enno.shioji@peerindex.com)
 */
public class ParseTweet extends BaseFunction {
    private static final Logger log = LoggerFactory.getLogger(ParseTweet.class);

    private ContentExtracter extracter;

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        Status parsed = parse((String)tuple.get(0));
        User user = parsed.getUser();
        String userScreenName = user.getScreenName();

        StringBuilder tweetText = new StringBuilder();
        HashtagEntity[] hashtagEntities = parsed.getHashtagEntities();

        if(hashtagEntities != null) {
            for(HashtagEntity entity : hashtagEntities) {
                tweetText.append(entity.getText() + " ");
            }
        }
        collector.emit(new Values(tweetText.toString(), parsed.getId()));
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
