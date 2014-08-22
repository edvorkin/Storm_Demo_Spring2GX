package com.spring2gx.storm

import backtype.storm.spout.SpoutOutputCollector
import backtype.storm.task.TopologyContext
import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.topology.base.BaseRichSpout
import backtype.storm.tuple.Fields
import backtype.storm.tuple.Values
import backtype.storm.utils.Utils
import groovy.util.logging.Slf4j

/**
 * Created by edvorkin on 8/11/2014.
 */
@Slf4j
class SentenceSpout extends BaseRichSpout {

  SpoutOutputCollector collector
    def index=0

   @Override
    void nextTuple() {
         String msgId=UUID.randomUUID().toString()
         this.collector.emit(new Values(sentences[index]),msgId)
            index++
            if (index>=sentences.size()){
                index=0
            }
    }

    @Override
    void ack(Object msgId) {}

    @Override
    void fail(Object msgId) {}

    @Override
    void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("sentence"))
    }
    @Override
    void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector=collector
    }


    def sentences=["Two households, both alike in dignity",
                   "In fair Verona, where we lay our scene",
                   "From ancient grudge break to new mutiny",
                   "Where civil blood makes civil hands unclean.",
                   "From forth the fatal loins of these two foes",
                   "A pair of star-cross'd lovers take their life",
                   "Whose misadventured piteous overthrows",
                   "Do with their death bury their parents' strife.",
                   "The fearful passage of their death-mark'd love",
                   "And the continuance of their parents' rage",
                   "Which, but their children's end, nought could remove",
                   "Is now the two hours' traffic of our stage",
                   "The which if you with patient ears attend",
                   "What here shall miss, our toil shall strive to mend."]
}
