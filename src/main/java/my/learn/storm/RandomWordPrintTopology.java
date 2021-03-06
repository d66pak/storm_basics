package my.learn.storm;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import my.learn.storm.bolts.StdoutPrintBolt;
import my.learn.storm.spouts.RandomWordGeneratorSpout;

/**
 * Runs in Storm remote mode.
 * Expects topology name as first argument
 *
 * RandomWordGeneratorSpout --> StdoutPrintBolt
 */
public class RandomWordPrintTopology
{
    public static void main( String[] args ) throws Exception
    {
        if (args.length < 1) {

            System.err.println("Missing topology name!");
            System.exit(1);
        }

        String topologyName = args[0];

        System.out.println("Creating remote topology : " + topologyName);

        TopologyBuilder topologyBuilder = new TopologyBuilder();

        // Spout creation
        topologyBuilder.setSpout(
                "random_word_spout",
                new RandomWordGeneratorSpout(), 1).setNumTasks(2);

        // Bolt creation

        /**
         *  Bolt with input field name

        List<String> fieldList = new ArrayList<>();
        fieldList.add("word");
        topologyBuilder.setBolt(
                "stdout_print_bolt",
                new StdoutPrintBolt(fieldList), 1).shuffleGrouping("random_word_spout");
         */

        /**
         * Bolt without input field name

        topologyBuilder.setBolt("stdout_print_bolt", new StdoutPrintBolt(), 1)
                .shuffleGrouping("random_word_spout");
         */

        /**
         * Increasing TASK count
         */
        topologyBuilder.setBolt("stdout_print_bolt", new StdoutPrintBolt(), 1)
                .setNumTasks(2) // No of tasks = 2
                .shuffleGrouping("random_word_spout");

        // Config settings
        Config config = new Config();
        config.setDebug(true);
        config.setNumWorkers(2);

        // Submit the topology
        StormSubmitter.submitTopology(topologyName, config, topologyBuilder.createTopology());
    }
}
