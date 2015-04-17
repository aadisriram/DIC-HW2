package storm.starter.trident.project.countmin; 

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.FilterNull;
import storm.trident.operation.builtin.MapGet;
import storm.trident.operation.builtin.Sum;
import storm.trident.state.StateFactory;
import storm.trident.testing.MemoryMapState;
import storm.trident.testing.Split;
import storm.trident.testing.FixedBatchSpout;
import backtype.storm.tuple.Values;


import storm.trident.operation.builtin.Count;

import storm.starter.trident.project.countmin.state.CountMinSketchStateFactory;
import storm.starter.trident.project.countmin.state.CountMinQuery;
import storm.starter.trident.project.countmin.state.CountMinSketchUpdater;
import storm.starter.trident.project.countmin.state.TopKQuery;
import storm.starter.trident.project.functions.BloomFilter;

import storm.starter.trident.project.functions.SentenceBuilder;
import storm.starter.trident.project.spouts.TwitterSampleSpout;
import storm.starter.trident.project.functions.ParseTweet;
import storm.starter.trident.project.functions.Tweet;

import storm.starter.trident.project.functions.NormalizeText;

import backtype.storm.spout.ShellSpout;
import backtype.storm.topology.*;
import java.util.Map;

import storm.trident.spout.IBatchSpout;
import backtype.storm.task.TopologyContext;
import storm.trident.operation.TridentCollector;

import org.apache.storm.hdfs.trident.*;
import org.apache.storm.hdfs.trident.rotation.*;
import org.apache.storm.hdfs.trident.format.*;

import backtype.storm.StormSubmitter;

import storm.kafka.trident.*;
import storm.kafka.*;
import backtype.storm.spout.SchemeAsMultiScheme;

/**
 *@author: Preetham MS (pmahish@ncsu.edu)
 *Modified by Aaditya Sriram (asriram4@ncsu.edu)
 */


public class CountMinSketchTopology {

	public static StormTopology buildTopology( String[] filterWords, LocalDRPC drpc ) {

        TridentTopology topology = new TridentTopology();

        int width = 100;
		int depth = 150;
		int seed = 100;
		int topk_count = 10;

		//Fetch the twitter credentials from Env variables
		String consumerKey = System.getenv("TWITTER_CONSUMER_KEY");
    	String consumerSecret = System.getenv("TWITTER_CONSUMER_SECRET");
    	String accessToken = System.getenv("TWITTER_ACCESS_TOKEN_KEY");
    	String accessTokenSecret = System.getenv("TWITTER_ACCESS_TOKEN_SECRET");

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
			.each(new Fields("tweet"), new ParseTweet(), new Fields("text", "tweetId"))
			//extract just the tweet from the three fields we have
			.each(new Fields("text", "tweetId"), new SentenceBuilder(), new Fields("sentence"))
			//Split the sentence into words
			.each(new Fields("sentence"), new Split(), new Fields("wordsl"))
			//Normalize the text by converting to lower case and removing non alphabets and numbers
			.each(new Fields("wordsl"), new NormalizeText(), new Fields("lWords"))
			//Pass the data through a BloomFilter and remove stop words
			.each(new Fields("lWords"), new BloomFilter(), new Fields("words"))
			//Filter the null
			.each(new Fields("words"), new FilterNull())
			//Add the text into our persistent store MinSketch
			.partitionPersist(new CountMinSketchStateFactory(depth, width, seed, topk_count), new Fields("words"), new CountMinSketchUpdater())
			;

		//Call this to get the count of words passed in the query
		topology.newDRPCStream("get_count", drpc)
			//Split the query to find counts for each of the words in the query
			.each(new Fields("args"), new Split(), new Fields("query"))
			.stateQuery(countMinDBMS, new Fields("query"), new CountMinQuery(), new Fields("count"))
			.project(new Fields("query", "count"))
			;

		//Call this to get the top-K words
		topology.newDRPCStream("get_topk", drpc)
			//Call the top k query and print the returned list
			.stateQuery(countMinDBMS, new Fields("args"), new TopKQuery(), new Fields("topk"))
			.project(new Fields("topk"))
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
            System.out.println("DRPC RESULT: " + drpc.execute("get_count","love hate"));

            //Query type to get the top-k items
            System.out.println("DRPC RESULT TOPK: " + drpc.execute("get_topk",""));
            Thread.sleep(3000);
        }

		System.out.println("STATUS: OK");
		//cluster.shutdown();
        //drpc.shutdown();
	}
}
