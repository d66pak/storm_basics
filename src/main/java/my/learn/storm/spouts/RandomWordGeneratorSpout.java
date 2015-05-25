package my.learn.storm.spouts;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Created on 5/24/15.
 * TODO: Add logging
 */
public class RandomWordGeneratorSpout implements IRichSpout {

    private static final long serialVersionUID = 1L;
    private static final String RANDOM_WORDS = "learnmetal polyethenemoldy segmentconstant nyepolish warningchromate " +
            "pelvicjangle ethologyirregular flexintegral networkdevice doubleride hatedcows mayonnaisemutes " +
            "profilegarlic dronefounder immodestbrunnich allplace sizzlegracious firstchase estuaryinner finitegamete " +
            "circlewilling tropicalpooh leggedweepy pinbean sufferorogeny spilltottering wandercluster wiseperiod " +
            "pendulumlegged steepvega camcircular bouncyavocado demandpatter spiculesbruised pastethirsty pubicgirth " +
            "hoistknead joinsunburn rewardingroundup onlysorry rantmellow cuckoldact requestline eggclopping " +
            "occiputunripe wavestattered romppremium demureharmful canoeingfilk liberatedamazing salsaplate " +
            "ballscenozoic friesiansnowboard oxinterfere niftyruler planetaryballs cookiessurd guillemotattractive " +
            "errantflapping goaliekneel rockperturb infectedfirst inspectbandanas arrowtrapdoor westernforthright " +
            "smokedinjuries lookquark signsunlucky bitepacking chestsimilar foggysilly cepheidseemly dcbroom " +
            "unfitliquid rashpanda sandstoneincomplete cetusplage upsetinterject macefurbelows batteredstamp " +
            "bairdslioch spudsauteed galaxyfire unpleasantrhenium macawperseus virtueinsomnia sticksin atomsnobbish " +
            "diamoxmiserly dispenserfoam bitbuzzer rashersmass magpiescalar galvanisingimpala cowardtowels wedreaction " +
            "pottedchondrule opticsclawed awaresilica lacesaerobic";

    private List<String> mRandomWordList;
    private SpoutOutputCollector mOutputCollector;
    private final Random mRandom = new Random();

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

        declarer.declare(new Fields("word"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {

        System.out.println("Open called...");
        mOutputCollector = collector;
        mRandomWordList = Arrays.asList(RANDOM_WORDS.split(" "));
    }

    @Override
    public void close() {

        System.out.println("Closing Random Word Generator spout...");
    }

    @Override
    public void activate() {

        System.out.println("Activating Random Word Generator spout...");
    }

    @Override
    public void deactivate() {

        System.out.println("Deactivating Random Word Generator spout...");
    }

    @Override
    public void nextTuple() {

        /**
         * Fetch random word from the list and emit
         * TODO: Add msgId while emiting
         */

        String randomWord = mRandomWordList.get(mRandom.nextInt(mRandomWordList.size()));
        mOutputCollector.emit(new Values(randomWord));
    }

    @Override
    public void ack(Object msgId) {

        System.out.println("Received ACK for tuple with msgId : " + msgId.toString());
    }

    @Override
    public void fail(Object msgId) {

        System.out.println("FAIL msgId : " + msgId.toString());
    }
}
