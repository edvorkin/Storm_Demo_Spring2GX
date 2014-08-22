import backtype.storm.Testing
import backtype.storm.task.OutputCollector
import backtype.storm.task.TopologyContext
import backtype.storm.testing.MkTupleParam
import backtype.storm.tuple.Values
import com.spring2gx.storm.SplitSentenceBolt
import spock.lang.Specification


/**
 * Created by edvorkin on 8/12/2014.
 */
class SplitSentenceBoltTest extends Specification {

    def "splitting sentence test - interactions"() {
        given:
            MkTupleParam param = new MkTupleParam();
            param.setFields("sentence");
            OutputCollector outputCollector=Mock()
            TopologyContext topologyContext=Mock()
            SplitSentenceBolt splitSentenceBolt=new SplitSentenceBolt()
            splitSentenceBolt.prepare([:],topologyContext,outputCollector)
            backtype.storm.tuple.Tuple tuple = Testing
                    .testTuple(new Values(sentence),param);
        when:
            splitSentenceBolt.execute(tuple)
        then:
            9 * outputCollector.emit(_)
            1 * outputCollector.ack(_)
            0 * outputCollector.fail(_)
        where:
        sentence="From forth the fatal loins of these two foes"
    }
    def "splitting sentence test"() {
        given:
        MkTupleParam param = new MkTupleParam();
        param.setFields("sentence");
        TestOutputCollector testOutputCollector=new TestOutputCollector()
        TopologyContext topologyContext=Mock()
        SplitSentenceBolt splitSentenceBolt=new SplitSentenceBolt()
        splitSentenceBolt.prepare([:],topologyContext,testOutputCollector)
        backtype.storm.tuple.Tuple tuple = Testing
                .testTuple(new Values(sentence),param);
        when:
        splitSentenceBolt.execute(tuple)
        then:
        splitSentenceBolt.collector.out == ["From", "forth", "the",
                                            "fatal", "loins",
                                            "of", "these", "two", "foes"]
        where:
        sentence="From forth the fatal loins of these two foes"


    }
}