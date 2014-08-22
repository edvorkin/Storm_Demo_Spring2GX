package com.spring2gx.storm.trident

import backtype.storm.tuple.Values
import groovy.util.logging.Slf4j
import storm.trident.operation.BaseFunction
import storm.trident.operation.TridentCollector
import storm.trident.tuple.TridentTuple


/**
 * Created by edvorkin on 8/14/2014.
 */
@Slf4j
class SplitSentence extends BaseFunction {
    @Override
    void execute(TridentTuple tuple, TridentCollector collector) {
        String sentence=tuple.getStringByField("sentence")
        sentence.split().each { word->
            collector.emit(new Values(word))
        }
    }
}
