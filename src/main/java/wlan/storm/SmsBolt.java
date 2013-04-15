package wlan.storm;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

/**
 * Created with IntelliJ IDEA.
 * User: yangzq2
 * Date: 13-4-11
 * Time: 下午2:57
 */
public class SmsBolt extends BaseBasicBolt {
    private Logger logger = LoggerFactory.getLogger(SmsBolt.class);
    private Logger countLogger = LoggerFactory.getLogger("tourist.count");

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String imsi = input.getString(0);
        long time = input.getLong(1);
        try {
            logger.info(String.format("Send sms to:%s on signal time:%s/%s ", imsi, getTime(time), time));
            countLogger.info(String.format("%s:%s/%s", imsi, getTime(time), time));
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    private static String getTime(long s) throws ParseException {
        return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date(s - TimeZone.getDefault().getRawOffset()));
    }
}
