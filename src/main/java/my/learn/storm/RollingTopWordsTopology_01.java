package my.learn.storm;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import my.learn.storm.spouts.RandomWordGeneratorSpout;
import storm.starter.bolt.IntermediateRankingsBolt;
import storm.starter.bolt.RollingCountBolt;
import storm.starter.bolt.TotalRankingsBolt;

/**
 * Created on 6/14/15.
 *
 * Same as RollingTopWords.java of storm-starter except that this one uses
 * RandomWordGeneratorSpout
 *
 * Expects topology name as topology name
 *
 * RandomWordGeneratorSpout --> RollingCountBolt --> IntermediateRankingsBolt --> TotalRankingsBolt
 */
public class RollingTopWordsTopology_01 {

    public static void main(String[] args) throws Exception {

        if (args.length < 1) {

            System.err.println("Missing topology name!");
            System.exit(1);
        }

        String topologyName = args[0];

        System.out.println("Creating remote topology : " + topologyName);

        int topNWords = 5;
        String spoutId = "random_word_spout";
        String counterId = "rolling_counter_bolt";
        String intermediateRankerId = "intermediate_ranker_bolt";
        String totalRankerId = "total_ranker_bolt";

        TopologyBuilder builder = new TopologyBuilder();

        /**
         * No need to specify number of tasks
         * Default is 1 task per 1 executor
         */
        builder.setSpout(spoutId, new RandomWordGeneratorSpout(0), 5).setNumTasks(5);

        /**
         * Window size = 10 (secs)
         * Emit frequency = every 5 (secs)
         */
        builder.setBolt(counterId, new RollingCountBolt(10, 5), 4)
                .fieldsGrouping(spoutId, new Fields("word"));


        /**
         * TopN = 5 top words
         */
        builder.setBolt(intermediateRankerId, new IntermediateRankingsBolt(topNWords), 2)
                .fieldsGrouping(counterId, new Fields("obj"));

        /**
         * TopN = 5 top words
         *
         * Only 1 executor to consolidate the result
         */
        builder.setBolt(totalRankerId, new TotalRankingsBolt(topNWords), 1)
                .globalGrouping(intermediateRankerId);

        Config conf = new Config();
        conf.setNumWorkers(4);
        conf.setDebug(true);

        StormSubmitter.submitTopologyWithProgressBar(topologyName, conf, builder.createTopology());
    }
}
