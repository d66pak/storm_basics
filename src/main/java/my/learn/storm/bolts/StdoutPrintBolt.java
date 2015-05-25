package my.learn.storm.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import java.util.List;
import java.util.Map;

/**
 * Created on 5/25/15.
 * TODO: Add logging
 */
public class StdoutPrintBolt implements IRichBolt {

    private static final long serialVersionUID = 1L;

    private List<String> mInputFields;

    public StdoutPrintBolt(List<String> inputFields) {

        mInputFields = inputFields;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

        System.out.println("Prepare called...");
    }

    @Override
    public void execute(Tuple input) {

        for (String field : mInputFields) {

            System.out.println(field + " : " + input.getStringByField(field));
        }
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
