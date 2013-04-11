package ref.storm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wlan.storm.SignallingSpout;
import ref.util.MetricsDetector;
import ref.util.TouristDetector;

import java.util.Map;

import static java.lang.String.format;

/**
 * 当发现游客时，输出游客+1，当发现不是游客时，输出游客-1
 */
public class TouristCountChangeBolt extends BaseRichBolt implements TouristDetector.Listener {
    private Logger logger = LoggerFactory.getLogger(TouristCountChangeBolt.class);
    private TouristDetector detector;
    private OutputCollector outputCollector;

    public TouristCountChangeBolt(MetricsDetector.Metrics... metricses) {
        detector = new TouristDetector(this, metricses);
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        if (tuple.getSourceStreamId().equals(SignallingSpout.SIGNALLING)) {
            if (logger.isInfoEnabled()){
                logger.info(format("[%s]:%s,%s", SignallingSpout.SIGNALLING, tuple.getString(0), tuple.getLong(1)));
                logger.info(format("[%s]:%s", SignallingSpout.SIGNALLING, tuple.toString()));
            }
            String imsi = tuple.getString(0);
            long time = tuple.getLong(1);
            String loc = tuple.getString(2);
            String cell = tuple.getString(3);
            detector.onSignaling(imsi, time, loc, cell);
        } else if (tuple.getSourceStreamId().equals(UpdateTimeBolt.UPDATE_TIME)) {
            if (logger.isInfoEnabled()){
                logger.info(format("[%s]:%s", UpdateTimeBolt.UPDATE_TIME, tuple.toString()));
            }
            long time = tuple.getLong(0);
            detector.updateTime(time);
        }
        this.outputCollector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("delta", "time", "imsi"));
    }

    @Override
    public void addTourist(String imsi, long time) {
        Values tuple = new Values(+1,time,imsi);
        if (logger.isInfoEnabled()) {
            logger.info(format("add tourist: %s", imsi));
            logger.info(format("[%s]:%s", Utils.DEFAULT_STREAM_ID, tuple.toString()));
        }
        outputCollector.emit(tuple);
    }

    @Override
    public void removeTourist(String imsi, long time) {
        Values tuple = new Values(-1,time,imsi);
        if (logger.isInfoEnabled()) {
            logger.info(format("remove tourist: %s", imsi));
            logger.info(format("[%s]:%s", Utils.DEFAULT_STREAM_ID, tuple.toString()));
        }
        outputCollector.emit(tuple);
    }
}
