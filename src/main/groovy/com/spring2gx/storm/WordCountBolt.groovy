package com.spring2gx.storm

import backtype.storm.task.OutputCollector
import backtype.storm.task.TopologyContext
import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.topology.base.BaseRichBolt
import backtype.storm.tuple.Fields
import backtype.storm.tuple.Values
import groovy.util.logging.Slf4j

/**
 * Created by edvorkin on 8/11/2014.
 */
@Slf4j
class WordCountBolt extends BaseRichBolt{
    HashMap counts=null
    OutputCollector outputCollector;
    @Override
    void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word","count"))
    }
    @Override
    void prepare(Map stormConf, TopologyContext context,
                            OutputCollector collector) {
        this.outputCollector=collector
        counts=[:]
    }
 @Override
    void execute(backtype.storm.tuple.Tuple tuple) {
        String word=tuple.getStringByField("word")
        Long count=this.counts.get(word)
        if (count==null) {
            count=0L
        }
        count++
        this.counts[word]=count
        this.outputCollector.emit(new Values(word, count))

   }

}
