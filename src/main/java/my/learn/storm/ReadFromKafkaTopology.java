package my.learn.storm;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import my.learn.storm.bolts.StdoutPrintBolt;
import org.joda.time.DateTime;
import storm.kafka.*;

/**
 * Created on 7/4/15.
 *
 * Uses storm-kafka library to read a topic from Kafka
 */
public class ReadFromKafkaTopology {

    public static void main(String[] args) throws Exception {

        if (args.length < 1) {

            System.err.println("Missing topology name!");
            System.exit(1);
        }

        String topologyName = args[0];

        String zkHostPort = "localhost:2181";
        String topic = "j_topic_00";
        String zkRoot = "/storm_kafka";
        String spoutId = "kafka_spout_01";

        BrokerHosts hosts = new ZkHosts(zkHostPort);
        SpoutConfig spoutConfig = new SpoutConfig(hosts, topic, zkRoot, spoutId);
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

        // Setting time to specific time did not work
        // DateTime dateTime = new DateTime(2015, 7, 5, 0, 30);
        // System.out.println("Setting date : " + dateTime.toString());
        // spoutConfig.startOffsetTime = dateTime.getMillis();

        spoutConfig.startOffsetTime = kafka.api.OffsetRequest.EarliestTime();
        // spoutConfig.startOffsetTime = kafka.api.OffsetRequest.LatestTime();
        // spoutConfig.forceFromStart = true;

        /**
         * Observations:
         *
         * NOTE: Make sure the bolts connected to KafkaSpout ack the tuples else,
         * the spout will replay them.
         *
         * 1.
         * - New Kafka spout launched very first time
         * - startOffsetTime & forceFromStart left default not modified
         * - Kafka topic has already some logs in it
         *
         * Result:
         * - KafkaSpout cannot find offset info from the given zkRoot
         * - Starts from the LAST offset i.e. newest log in kafka
         *
         * 2.
         * - New Kafka spout launched very first time
         * - startOffsetTime = kafka.api.OffsetRequest.EarliestTime()
         * - forceFromStart left default (false)
         * - Kafka topic has already some logs in it
         *
         * Result:
         * - KafkaSpout cannot find offset info from the given zkRoot
         * - Expected: Starts form the FIRST offset i.e. oldest log in kafka
         * - *Started from LAST offset*
         * - I guess forceFromStart should be set to true for it to read from first offset
         *
         * 3.
         * - Same Kafka spout has been launched previously
         * - Kafka topic has already some logs in it
         * - startOffsetTime = kafka.api.OffsetRequest.EarliestTime()
         * - forceFromStart = true
         *
         * Result:
         * - KafkaSpout finds offset info from the given zkRoot
         * - Starts form the FIRST offset i.e. oldest log in kafka
         *
         * 4.
         * - Same Kafka spout has been launched previously
         * - Kafka topic has already some logs in it
         * - startOffsetTime = kafka.api.OffsetRequest.LatestTime()
         * - forceFromStart = true
         *
         * Result:
         * - KafkaSpout finds offset info from the given zkRoot
         * - Starts from the LAST offset i.e. newest log in kafka
         */

        /**
         * Quote from : https://github.com/apache/storm/tree/master/external/storm-kafka
         *
         * As the topology runs the Kafka spout keeps track of the offsets it has read and emitted by storing state
         * information under the ZooKeeper path SpoutConfig.zkRoot+ "/" + SpoutConfig.id. In the case of failures it
         * recovers from the last written offset in ZooKeeper.
         *
         * Important: When re-deploying a topology make sure that the settings for SpoutConfig.zkRoot and
         * SpoutConfig.id were not modified, otherwise the spout will not be able to read its previous consumer state
         * information (i.e. the offsets) from ZooKeeper -- which may lead to unexpected behavior and/or to data loss,
         * depending on your use case.
         *
         * This means that when a topology has run once the setting KafkaConfig.startOffsetTime will not have an effect
         * for subsequent runs of the topology because now the topology will rely on the consumer state information
         * (offsets) in ZooKeeper to determine from where it should begin (more precisely: resume) reading.
         *
         * If you want to force the spout to ignore any consumer state information stored in ZooKeeper, then you should
         * set the parameter KafkaConfig.ignoreZkOffsets [KafkaConfig.forceFromStart in older version] to true.
         *
         * If true, the spout will always begin reading from the offset defined by KafkaConfig.startOffsetTime
         * as described above.
         */
        KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(spoutId, kafkaSpout, 1);

        builder.setBolt("stdout_bolt", new StdoutPrintBolt(), 1).shuffleGrouping(spoutId);

        System.out.println("Creating topology : " + topologyName);

        Config conf = new Config();
        conf.setDebug(true);
        conf.setNumWorkers(2);

        StormSubmitter.submitTopology(topologyName, conf, builder.createTopology());

    }
}
