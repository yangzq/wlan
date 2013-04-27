package wlan.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

/**
 * Created with IntelliJ IDEA.
 * User: yangzq2
 * Date: 13-3-13
 * Time: 下午6:34
 */
public class WlanTopology {
//    private static final long ONE_HOUR = 60 * 60 * 1000;

    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
        TopologyBuilder builder = getTopologyBuilder();
        Config conf = new Config();
        conf.setDebug(true);

        if (args!=null && args.length > 0) { // 远程模式
            System.out.println("Remote mode");
            conf.setNumWorkers(10);
            conf.setMaxSpoutPending(100);
            conf.setNumAckers(5);
            conf.setMessageTimeoutSecs(5);
            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        } else {
            // 本地模式，调试代码
            System.out.println("Local mode");
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("wlanTopology", conf, builder.createTopology());

            Utils.sleep(60000);
            cluster.shutdown();
        }
    }

    public static TopologyBuilder getTopologyBuilder() {
        TopologyBuilder builder = new TopologyBuilder();
        String signallingSpout1 = "signallingSpout1";
        String signallingSpout2 = "signallingSpout2";
        String preconditionBolt = "preconditionBolt";
        String wapBolt = "wapBolt";
        String smsBolt = "smsBolt";
        builder.setSpout(signallingSpout1, new SignallingSpout(5002));
        builder.setSpout(signallingSpout2, new SignallingSpout(5003));

        builder.setBolt(preconditionBolt, new PreconditionBolt(), 1)
                .fieldsGrouping(signallingSpout1, SignallingSpout.SIGNALLING, new Fields("imsi"))
                .fieldsGrouping(signallingSpout2, SignallingSpout.SIGNALLING, new Fields("imsi"));
        builder.setBolt(wapBolt, new WapBolt(), 1)
                .fieldsGrouping(preconditionBolt, PreconditionBolt.PRECONDITION, new Fields("imsi"))
//                .allGrouping(preconditionBolt, PreconditionBolt.UPDATETIME);
                .fieldsGrouping(preconditionBolt, PreconditionBolt.UPDATETIME, new Fields("imsi"));
        builder.setBolt(smsBolt, new SmsBolt(), 1)
                .globalGrouping(wapBolt, WapBolt.WAPSTREAM);
        return builder;
    }
}
