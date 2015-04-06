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
import twitter4j.HashtagEntity;

/**
 * @author Aaditya Sriram
 */
public class ParseTweet extends BaseFunction {

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {

        Status parsed = (Status)tuple.get(0);
        User user = parsed.getUser();
        String userScreenName = user.getScreenName();

        String tweetText = new String();
        HashtagEntity[] hashtagEntities = parsed.getHashtagEntities();

        if(hashtagEntities != null) {
            for(HashtagEntity entity : hashtagEntities) {
                tweetText += entity.getText() + " ";
            }
        }
        collector.emit(new Values(tweetText, parsed.getId(), userScreenName));
    }
}
