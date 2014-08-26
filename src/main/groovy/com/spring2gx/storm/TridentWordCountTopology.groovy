package com.spring2gx.storm

import backtype.storm.Config
import backtype.storm.LocalCluster
import backtype.storm.StormSubmitter
import backtype.storm.tuple.Fields
import backtype.storm.tuple.Values
import com.spring2gx.storm.trident.SplitSentence
import storm.trident.TridentTopology
import storm.trident.operation.builtin.Count
import storm.trident.operation.builtin.Debug
import storm.trident.testing.FixedBatchSpout
import storm.trident.testing.MemoryMapState
import storm.trident.testing.Split

FixedBatchSpout spout=new FixedBatchSpout(new Fields("sentence"), 5,
        new Values("Two households, both alike in dignity"),
        new Values("In fair Verona, where we lay our scene"),
        new Values("From ancient grudge break to new mutiny"),
        new Values("Where civil blood makes civil hands unclean."),
        new Values("From forth the fatal loins of these two foes"),
        new Values("A pair of star-cross'd lovers take their life"),
        new Values("Whose misadventured piteous overthrows"),
        new Values("Do with their death bury their parents' strife."),
        new Values("The fearful passage of their death-mark'd love"),
        new Values("And the continuance of their parents' rage"),
        new Values("Which, but their children's end, nought could remove"),
        new Values("Is now the two hours' traffic of our stage"),
        new Values("The which if you with patient ears attend"),
        new Values("What here shall miss, our toil shall strive to mend."))
      spout.setCycle(true)



TridentTopology topology=new TridentTopology()
topology.newStream("stream",spout)
.each(new Fields("sentence"),new SplitSentence(),new Fields("word"))
.groupBy(new Fields("word"))
.persistentAggregate(new MemoryMapState.Factory(), new Count(),
        new Fields("count")).parallelismHint(6)
.newValuesStream().each(new Fields("word","count"),new Debug())

Config conf = new Config();
conf.setMaxSpoutPending(20);
if (args.length == 0) {
     LocalCluster cluster = new LocalCluster();
    cluster.submitTopology("wordCounter", conf, topology.build());

}
else {
    conf.setNumWorkers(3);
    StormSubmitter.submitTopology(args[0], conf, topology(null));
}