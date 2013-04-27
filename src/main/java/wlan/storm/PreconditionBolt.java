package wlan.storm;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wlan.util.KbUtils;

/**
 * 先决条件判断，当前用于判断手机是否有wifi功能
 * Created with IntelliJ IDEA.
 * User: yangzq2
 * Date: 13-3-13
 * Time: 下午6:41
 */
public class PreconditionBolt extends BaseBasicBolt {
    private Logger logger = LoggerFactory.getLogger(PreconditionBolt.class);
    private BasicOutputCollector outputCollector;
    public static final String PRECONDITION = "preconditionStream";
    public static final String UPDATETIME = "updateTimeStream";
    private long lastSignalTime = 0L;

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        this.outputCollector = collector;
        String imsi = tuple.getString(0);
        String eventType = tuple.getString(1);
        Long time = tuple.getLong(2);
        String lac = tuple.getString(3);
        String cell = tuple.getString(4);
        if (logger.isDebugEnabled()){
            logger.debug(String.format("[%s]%s,%s,%s", PRECONDITION, imsi, eventType, time));
        }
        Values values = new Values(imsi, eventType, time, lac, cell);
        boolean wifi = KbUtils.getInstance().checkWifi(imsi);
        if (wifi){
            outputCollector.emit(PRECONDITION, values);
        } else {
            if (logger.isDebugEnabled()){
                logger.debug(String.format("DO NOT support wifi:%s", imsi));
            }
        }
        if (time > lastSignalTime){
            outputCollector.emit(UPDATETIME, new Values(time, imsi));
            lastSignalTime = time;
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(PRECONDITION, new Fields("imsi", "eventType", "time", "lac", "cell"));
        outputFieldsDeclarer.declareStream(UPDATETIME, new Fields("time", "imsi"));
    }

}
