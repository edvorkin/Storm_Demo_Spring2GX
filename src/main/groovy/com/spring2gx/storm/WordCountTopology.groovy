package com.spring2gx.storm

import backtype.storm.Config
import backtype.storm.LocalCluster
import backtype.storm.StormSubmitter
import backtype.storm.topology.TopologyBuilder
import backtype.storm.tuple.Fields

/**
 * Created by edvorkin on 8/11/2014.
 */

    final String SENTENCE_SPOUT_ID="sentence_spout"
    final String SPLIT_BOLT_ID="split_bolt"
    final String COUNT_BOLT_ID="count_bolt"
    final String REPORT_BOLT_ID="report_bolt"
    final String TOPOLOGY_NAME="word-count-topology"






TopologyBuilder topologyBuilder=new TopologyBuilder()
topologyBuilder.setSpout(SENTENCE_SPOUT_ID, new SentenceSpout())
topologyBuilder.setBolt(SPLIT_BOLT_ID,new SplitSentenceBolt()).setNumTasks(2)
            .shuffleGrouping(SENTENCE_SPOUT_ID)
topologyBuilder.setBolt(COUNT_BOLT_ID, new WordCountBolt(),2)
            .fieldsGrouping(SPLIT_BOLT_ID,new Fields("word"))
topologyBuilder.setBolt(REPORT_BOLT_ID,new PrinterBolt())
            .globalGrouping(COUNT_BOLT_ID)
Config config=new Config()
config.setNumWorkers(2)

if (args != null && args.length > 0) {

    StormSubmitter.submitTopology(TOPOLOGY_NAME, config, topologyBuilder.createTopology());
} else {

    LocalCluster cluster = new LocalCluster()
    cluster.submitTopology(TOPOLOGY_NAME, config, topologyBuilder.createTopology())
    Thread.sleep(8000)
    cluster.killTopology(TOPOLOGY_NAME)
    cluster.shutdown()
}




