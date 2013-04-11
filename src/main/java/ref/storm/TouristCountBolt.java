package ref.storm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 汇总并输出游客数
 */
public class TouristCountBolt extends BaseRichBolt {
    private Logger countLogger = LoggerFactory.getLogger("tourist.count");
    private AtomicInteger count = new AtomicInteger();
    private OutputCollector outputCollector;


    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        int delta = tuple.getInteger(0);
        count.addAndGet(delta);
        long time = tuple.getLong(1);
        String imsi = tuple.getString(2);
        if (countLogger.isInfoEnabled()){
            try {
                countLogger.info(String.format("%s,%d,[%s],%s:%s", count.toString(), time, getTime(time), (delta == 1)?"+":"-", imsi));
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
        this.outputCollector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
    String getTime(long s) throws ParseException {
        return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date(s- TimeZone.getDefault().getRawOffset()));
    }
}
