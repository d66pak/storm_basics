package my.learn.storm.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created on 5/25/15.
 * TODO: Add logging
 */
public class StdoutPrintBolt implements IRichBolt {

    private static final long serialVersionUID = 1L;

    private List<String> mInputFields;
    private OutputCollector mCollector;

    /**
     * Default Constructor
     *
     * If input filed list is not provided then it is assumed that there
     * is only one field at index zero
     */
    public StdoutPrintBolt() {

        mInputFields = new ArrayList<String>(0);
    }

    public StdoutPrintBolt(List<String> inputFields) {

        mInputFields = inputFields;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

        System.out.println("Prepare called...");
        mCollector = collector;
    }

    @Override
    public void execute(Tuple input) {

        if (mInputFields.isEmpty()) {

            System.out.println("Instance : " + this + " Default field : " + input.getString(0));
        } else {

            for (String field : mInputFields) {

                System.out.println("Instance : " + this + " " + field + " : " + input.getStringByField(field));
            }
        }

        // If you don't ack then tuples will be replayed
        mCollector.ack(input);
    }

    @Override
    public void cleanup() {

        System.out.println("Cleanup called...");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

        // No tuples are emitted
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
