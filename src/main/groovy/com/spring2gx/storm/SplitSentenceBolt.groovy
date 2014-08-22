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
class SplitSentenceBolt extends BaseRichBolt {

    OutputCollector collector

    @Override
    void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"))
    }

    @Override
    void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector=collector
    }

    // TODO: optimize execute method
    @Override
    void execute(backtype.storm.tuple.Tuple tuple) {



    }


}
