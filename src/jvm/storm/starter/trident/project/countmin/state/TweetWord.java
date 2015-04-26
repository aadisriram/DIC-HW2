package storm.starter.trident.project.countmin.state;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/*
* @author : Aaditya Sriram
* Each tweetword is an object that hold a word from
* the processed tweet, along with the current count
* estimate.
*/
public class TweetWord implements Serializable {

	//The actual word
    private List<String> hashtags = new ArrayList<String>();

    //Stored count estimate from the count min sketch
    private long id;

    //Empty Default Constructor, just in case someone hates
    //using parameterized constructors
    public TweetWord() {

    }

    public void addHashtags(String hashtag) {
        hashtags.add(hashtag);
    }

    //Parameterized Constructor to initialize, duh
    public TweetWord(List<String> hashtag, long id) {
    	this.hashtags = hashtag;
    	this.id = id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TweetWord)) return false;

        TweetWord tweetWord = (TweetWord) o;

        if (id != tweetWord.id) return false;
        if (hashtags != null ? !hashtags.equals(tweetWord.hashtags) : tweetWord.hashtags != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = hashtags != null ? hashtags.hashCode() : 0;
        result = 31 * result + (int) (id ^ (id >>> 32));
        return result;
    }

    public List<String> getHashtags() {
        return hashtags;
    }

    public void setHashtags(List<String> hashtags) {
        this.hashtags = hashtags;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }
}