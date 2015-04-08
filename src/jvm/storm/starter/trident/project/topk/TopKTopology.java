package storm.starter.trident.project.topk; 

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.FilterNull;
import storm.trident.state.StateFactory;
import storm.trident.testing.MemoryMapState;
import storm.trident.testing.Split;
import storm.trident.testing.FixedBatchSpout;

import storm.starter.trident.project.topk.state.TopKStateFactory;
import storm.starter.trident.project.topk.state.TopKStateUpdater;
import storm.starter.trident.project.topk.state.TopKQuery;
import storm.starter.trident.project.functions.BloomFilter;

import storm.starter.trident.project.functions.SentenceBuilder;
import storm.starter.trident.project.spouts.TwitterSampleSpout;
import storm.starter.trident.project.functions.ParseTweet;

import storm.starter.trident.project.functions.NormalizeText;

import backtype.storm.topology.*;
import java.util.Map;

public class TopKTopology {

	public static StormTopology buildTopology( String[] filterWords, LocalDRPC drpc ) {

        TridentTopology topology = new TridentTopology();

		int topk_count = 5;

		//Fetch the twitter credentials from Env variables
		String consumerKey = System.getenv("TWITTER_CONSUMER_KEY");
    	String consumerSecret = System.getenv("TWITTER_CONSUMER_SECRET");
    	String accessToken = System.getenv("TWITTER_ACCESS_TOKEN_KEY");
    	String accessTokenSecret = System.getenv("TWITTER_ACCESS_TOKEN_SECRET");

        // Create Twitter's spout
		TwitterSampleSpout spoutTweets = new TwitterSampleSpout(consumerKey, consumerSecret,
									accessToken, accessTokenSecret, filterWords);;

		TridentState topKDBMS = topology.newStream("tweets", spoutTweets)
			.each(new Fields("tweet"), new ParseTweet(), new Fields("text"))
			//extract just the tweet from the three fields we have
			.each(new Fields("text"), new SentenceBuilder(), new Fields("sentence"))
			//Split the sentence into words
			.each(new Fields("sentence"), new Split(), new Fields("wordsl"))
			//Normalize the text by converting to lower case and removing non alphabets and numbers
			.each(new Fields("wordsl"), new NormalizeText(), new Fields("lwords"))
			//Pass the data through a BloomFilter and remove stop words
			.each(new Fields("lwords"), new BloomFilter(), new Fields("words"))
			//Filter the null
			.each(new Fields("words"), new FilterNull())
			//Add the text into our persistent store TopK
			.partitionPersist(new TopKStateFactory(topk_count), new Fields("words"), new TopKStateUpdater())
			//.parallelismHint(3)
			;

		//Call this to get the top-K words
		topology.newDRPCStream("get_topk", drpc)
			//Call the top k query and print the returned list
			.stateQuery(topKDBMS, new Fields("args"), new TopKQuery(), new Fields("topk"))
			.project(new Fields("topk"))
			;

		return topology.build();
	}

	public static void main(String[] args) throws Exception {
		Config conf = new Config();
        conf.setDebug( false );
        conf.setMaxSpoutPending( 10 );

        LocalCluster cluster = new LocalCluster();
        LocalDRPC drpc = new LocalDRPC();
        	
        String[] filterWords = args.clone();
        cluster.submitTopology("topk", conf, buildTopology(filterWords, drpc));

        for (int i = 0; i < 1000; i++) {
        	//Query type to get the count for certain words
            //System.out.println("DRPC RESULT: " + drpc.execute("get_count","love hate"));

            //Query type to get the top-k items
            System.out.println("Current TopK Hashtags : " + drpc.execute("get_topk",""));
            Thread.sleep(3000);
        }

		System.out.println("STATUS: OK");
		//cluster.shutdown();
        //drpc.shutdown();
	}
}
