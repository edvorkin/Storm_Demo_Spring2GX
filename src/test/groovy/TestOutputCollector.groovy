import backtype.storm.task.IOutputCollector
import backtype.storm.task.OutputCollector

/**
 * Created by edvorkin on 8/12/2014.
 */
class TestOutputCollector extends OutputCollector{
    List out=new ArrayList()
    def anchor

    TestOutputCollector(IOutputCollector delegate) {
        super(delegate)
    }
    @Override
    List<Integer> emit(List<Object> tuple) {
        out.add(tuple[0])
        return [0]
    }

    @Override
    List<Integer> emit(backtype.storm.tuple.Tuple anchor, List<Object> tuple) {
        this.anchor=anchor
        out.add(tuple)
        return [0]
    }



    @Override
    void ack(backtype.storm.tuple.Tuple input) {

    }
}
