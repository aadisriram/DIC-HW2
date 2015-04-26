package storm.starter.trident.project.countmin;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.tuple.Fields;
import org.apache.storm.hdfs.trident.HdfsState;
import org.apache.storm.hdfs.trident.HdfsStateFactory;
import org.apache.storm.hdfs.trident.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.trident.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.trident.format.FileNameFormat;
import org.apache.storm.hdfs.trident.format.RecordFormat;
import org.apache.storm.hdfs.trident.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.trident.rotation.FileSizeRotationPolicy;
import storm.kafka.BrokerHosts;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import storm.kafka.trident.TridentKafkaConfig;
import storm.starter.trident.project.countmin.state.InvertedIndexQuery;
import storm.starter.trident.project.countmin.state.InvertedIndexStateFactory;
import storm.starter.trident.project.countmin.state.InvertedIndexUpdater;
import storm.starter.trident.project.functions.BloomFilter;
import storm.starter.trident.project.functions.NormalizeText;
import storm.starter.trident.project.functions.ParseTweet;
import storm.starter.trident.project.spouts.TwitterSampleSpout;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.FilterNull;
import storm.trident.state.StateFactory;
import storm.trident.testing.Split;

/**
 *@author: Preetham MS (pmahish@ncsu.edu)
 *Modified by Aaditya Sriram (asriram4@ncsu.edu)
 */


public class CountMinSketchTopology {

	public static StormTopology buildTopology( String[] filterWords, LocalDRPC drpc ) {

        TridentTopology topology = new TridentTopology();

        int windowSize = 1000;

		//Fetch the twitter credentials from Env variables
		String consumerKey = System.getenv("TWITTER_CONSUMER_KEY");
    	String consumerSecret = System.getenv("TWITTER_CONSUMER_SECRET");
    	String accessToken = System.getenv("TWITTER_ACCESS_TOKEN_KEY");
    	String accessTokenSecret = sSystem.getenv("TWITTER_ACCESS_TOKEN_SECRET");

    	Fields hdfsFields = new Fields("tweetId", "words");
    	Fields tweetField = new Fields("tweet");

        FileNameFormat fileNameFormat = new DefaultFileNameFormat()
                 .withPath("/trident")
                 .withPrefix("trident")
                 .withExtension(".txt");

        RecordFormat recordFormat = new DelimitedRecordFormat()
                 .withFields(hdfsFields);

        FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(5.0f, FileSizeRotationPolicy.Units.MB);

        HdfsState.Options options = new HdfsState.HdfsFileOptions()
                .withFileNameFormat(fileNameFormat)
                .withRecordFormat(recordFormat)
                .withRotationPolicy(rotationPolicy)
                .withFsUrl("hdfs://localhost:54310");

        StateFactory factory = new HdfsStateFactory().withOptions(options);

		BrokerHosts zk = new ZkHosts("localhost");
		TridentKafkaConfig spoutConf = new TridentKafkaConfig(zk, "tweet_message");

		spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
		//OpaqueTridentKafkaSpout spoutTweets = new OpaqueTridentKafkaSpout(spoutConf);

        // Create Twitter's spout
		TwitterSampleSpout spoutTweets = new TwitterSampleSpout(consumerKey, consumerSecret,
									accessToken, accessTokenSecret, filterWords);

		 // TridentState state = topology
		 // 	.newStream("tweets", spoutTweets)
		 // 	.each(new Fields("tweet"), new ParseTweet(), new Fields("text", "tweetId", "user"))
		 // 	.each(new Fields("text", "tweetId", "user"), new SentenceBuilder(), new Fields("sentence"))
			// .each(new Fields("sentence"), new Split(), new Fields("wordsl"))
			// .each(new Fields("wordsl"), new NormalizeText(), new Fields("lWords"))
			// .each(new Fields("lWords"), new BloomFilter(), new Fields("words"))
			// .each(new Fields("words"), new FilterNull());
   //          .partitionPersist(factory, hdfsFields, new HdfsUpdater(), new Fields());

		TridentState countMinDBMS = topology.newStream("tweets", spoutTweets)
			.each(new Fields("tweet"), new ParseTweet(), new Fields("hashtags", "tweetId"))
			.each(new Fields("hashtags"), new NormalizeText(), new Fields("lWords"))
			//Pass the data through a BloomFilter and remove stop words
			.each(new Fields("lWords"), new BloomFilter(), new Fields("words"))
			//Filter the null
			.each(new Fields("words"), new FilterNull())
			//Add the text into our persistent store MinSketch
			.partitionPersist(new InvertedIndexStateFactory(windowSize), new Fields("words", "tweetId"), new InvertedIndexUpdater());

		//Call this to get the count of words passed in the query
		topology.newDRPCStream("get_tweets", drpc)
			//Split the query to find counts for each of the words in the query
			.each(new Fields("args"), new Split(), new Fields("query"))
			.stateQuery(countMinDBMS, new Fields("query"), new InvertedIndexQuery(), new Fields("tweetId"))
			.project(new Fields("tweetId"))
			;

		return topology.build();
	}

	public static void main(String[] args) throws Exception {
		Config conf = new Config();
        conf.setDebug( false );
        conf.setMaxSpoutPending( 10 );
        //conf.put(Config.STORM_CLUSTER_MODE, "distributed");

        LocalCluster cluster = new LocalCluster();
        LocalDRPC drpc = new LocalDRPC();
        	
        String[] filterWords = args.clone();
        conf.setNumWorkers(3);
      	//StormSubmitter.submitTopologyWithProgressBar("get_count", conf, buildTopology(filterWords, null));
        cluster.submitTopology("get_count", conf, buildTopology(filterWords, drpc));

        for (int i = 0; i < 100; i++) {
        	//Query type to get the count for certain words
            System.out.println("DRPC RESULT: " + drpc.execute("get_tweets","love"));

        }

		System.out.println("STATUS: OK");
		//cluster.shutdown();
        //drpc.shutdown();
	}
}
