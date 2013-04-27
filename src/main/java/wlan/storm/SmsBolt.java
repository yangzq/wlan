package wlan.storm;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wlan.util.TimeUtil;

/**
 * Created with IntelliJ IDEA.
 * User: yangzq2
 * Date: 13-4-11
 * Time: 下午2:57
 */
public class SmsBolt extends BaseBasicBolt {
    private Logger logger = LoggerFactory.getLogger(SmsBolt.class);
    private Logger countLogger = LoggerFactory.getLogger("wlan.count");

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String imsi = input.getString(0);
        long time = input.getLong(1);
        System.out.println(String.format("Send sms to:%s on signal time:%s/%s ", imsi, TimeUtil.getTime(time), time));
        logger.info(String.format("Send sms to:%s on signal time:%s/%s ", imsi, TimeUtil.getTime(time), time));
        countLogger.info(String.format("%s:%s/%s", imsi, TimeUtil.getTime(time), time));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

}
