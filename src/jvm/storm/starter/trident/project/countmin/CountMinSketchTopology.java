package storm.starter.trident.project.countmin;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.tuple.Fields;
import org.apache.storm.hdfs.trident.HdfsState;
import org.apache.storm.hdfs.trident.HdfsStateFactory;
import org.apache.storm.hdfs.trident.HdfsUpdater;
import org.apache.storm.hdfs.trident.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.trident.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.trident.format.FileNameFormat;
import org.apache.storm.hdfs.trident.format.RecordFormat;
import org.apache.storm.hdfs.trident.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.trident.rotation.FileSizeRotationPolicy;
import storm.kafka.BrokerHosts;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import storm.kafka.trident.OpaqueTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;
import storm.starter.trident.project.countmin.state.InvertedIndexQuery;
import storm.starter.trident.project.countmin.state.InvertedIndexStateFactory;
import storm.starter.trident.project.countmin.state.InvertedIndexUpdater;
import storm.starter.trident.project.filters.EmptyStringFilter;
import storm.starter.trident.project.filters.PrintFilter;
import storm.starter.trident.project.functions.BloomFilter;
import storm.starter.trident.project.functions.ConcatFunction;
import storm.starter.trident.project.functions.NormalizeText;
import storm.starter.trident.project.functions.ParseTweet;
import storm.starter.trident.project.spouts.TwitterSampleSpout;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.Filter;
import storm.trident.operation.builtin.FilterNull;
import storm.trident.state.StateFactory;
import storm.trident.testing.Split;

/**
 *@author Aaditya Sriram (asriram4@ncsu.edu)
 */


public class CountMinSketchTopology {

	public static StormTopology buildTopology( String[] filterWords, LocalDRPC drpc ) {

        TridentTopology topology = new TridentTopology();

        int windowSize = 20000;

		//Fetch the twitter credentials from Env variables
		String consumerKey = System.getenv("TWITTER_CONSUMER_KEY");
    	String consumerSecret = System.getenv("TWITTER_CONSUMER_SECRET");
    	String accessToken = System.getenv("TWITTER_ACCESS_TOKEN_KEY");
    	String accessTokenSecret = System.getenv("TWITTER_ACCESS_TOKEN_SECRET");

    	Fields hdfsFields = new Fields("newTweetId", "words");
    	Fields tweetField = new Fields("tweet");

        FileNameFormat fileNameFormat = new DefaultFileNameFormat()
                 .withPath("/home")
                 .withPrefix("val")
                 .withExtension(".txt");

        RecordFormat recordFormat = new DelimitedRecordFormat()
                 .withFields(hdfsFields);

        FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(100.0f, FileSizeRotationPolicy.Units.KB);

        HdfsState.Options options = new HdfsState.HdfsFileOptions()
                .withFileNameFormat(fileNameFormat)
                .withRecordFormat(recordFormat)
                .withRotationPolicy(rotationPolicy)
                .withFsUrl("hdfs://152.46.19.147:54310");

        StateFactory factory = new HdfsStateFactory().withOptions(options);

		BrokerHosts zk = new ZkHosts("152.46.20.223:2181");
		TridentKafkaConfig spoutConf = new TridentKafkaConfig(zk, "tweet_message");

		spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
		OpaqueTridentKafkaSpout spoutTweets = new OpaqueTridentKafkaSpout(spoutConf);

        TridentKafkaConfig spoutConfHdfs = new TridentKafkaConfig(zk, "tweet_message_hdfs");

        spoutConfHdfs.scheme = new SchemeAsMultiScheme(new StringScheme());
        OpaqueTridentKafkaSpout spoutTweetsHdfs = new OpaqueTridentKafkaSpout(spoutConfHdfs);

//        Create Twitter's spout
//		TwitterSampleSpout spoutTweets = new TwitterSampleSpout(consumerKey, consumerSecret,
//									accessToken, accessTokenSecret, filterWords);

        InvertedIndexStateFactory stateFactory = new InvertedIndexStateFactory(windowSize);

		 TridentState state = topology.newStream("tweets", spoutTweetsHdfs)
             .each(spoutTweetsHdfs.getOutputFields(), new ParseTweet(), new Fields("hashtags", "tweetId", "username"))
//            .each(new Fields("tweet"), new ParseTweet(), new Fields("hashtags", "tweetId"))
             .each(new Fields("tweetId", "username"), new ConcatFunction(), new Fields("newTweetId"))
             .each(new Fields("hashtags"), new Split(), new Fields("hashtag"))
             .each(new Fields("hashtag"), new FilterNull())
//             .each(new Fields("hashtag"), new NormalizeText(), new Fields("lWords"))
                     //Pass the data through a BloomFilter and remove stop words
             .each(new Fields("hashtag"), new BloomFilter(), new Fields("words"))
                     //Filter the null
             .each(new Fields("words"), new FilterNull())
             .each(new Fields("words"), new EmptyStringFilter())
            .partitionPersist(factory, hdfsFields, new HdfsUpdater(), new Fields());

		TridentState countMinDBMS = topology.newStream("tweets", spoutTweets)
            .each(spoutTweets.getOutputFields(), new ParseTweet(), new Fields("hashtags", "tweetId", "username"))
//            .each(new Fields("tweet"), new ParseTweet(), new Fields("hashtags", "tweetId", "username"))
            .each(new Fields("hashtags"), new Split(), new Fields("hashtag"))
            .each(new Fields("hashtag"), new FilterNull())
//			.each(new Fields("hashtag"), new NormalizeText(), new Fields("lWords"))
            //Pass the data through a BloomFilter and remove stop words
			.each(new Fields("hashtag"), new BloomFilter(), new Fields("words"))
			//Filter the null
			.each(new Fields("words"), new FilterNull())
            .each(new Fields("words"), new EmptyStringFilter())
//            .each(new Fields("words", "tweetId", "username"), new PrintFilter())
            //Add the text into our persistent store MinSketch
            .partitionPersist(stateFactory, new Fields("words", "tweetId", "username"), new InvertedIndexUpdater());

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
        StormSubmitter.submitTopologyWithProgressBar("get_count", conf, buildTopology(filterWords, null));
//        cluster.submitTopology("get_count", conf, buildTopology(filterWords, drpc));
//
//        while(true) {
//        	//Query type to get the count for certain words
//            System.out.println("DRPC RESULT: " + drpc.execute("get_tweets","love"));
//            Thread.sleep(3000);
//        }

//		System.out.println("STATUS: OK");
		//cluster.shutdown();
        //drpc.shutdown();
	}
}
