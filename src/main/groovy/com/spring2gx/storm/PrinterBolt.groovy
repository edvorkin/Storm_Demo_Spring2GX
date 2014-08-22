package com.spring2gx.storm

import backtype.storm.Constants
import backtype.storm.task.OutputCollector
import backtype.storm.task.TopologyContext
import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.topology.base.BaseRichBolt

/**
 * Created by edvorkin on 8/11/2014.
 */

class PrinterBolt extends BaseRichBolt {
    HashMap counts=null
    @Override
    void declareOutputFields(OutputFieldsDeclarer declarer) {
    }
    @Override
    void prepare(Map stormConf, TopologyContext context,
                                OutputCollector collector) {
        this.counts=[:]
    }
    @Override
    void execute(backtype.storm.tuple.Tuple tuple) {
            String word = tuple.getStringByField("word")
            Long count = tuple.getLongByField("count")
            counts.put(word,count)
        }
    @Override
    void cleanup() {
        println "final count"
        counts.sort{ it.value}.reverseEach { k, v ->
            println  "'$k' -  $v "
        }}}
