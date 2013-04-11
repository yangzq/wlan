package ref.storm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static java.lang.String.format;

public class UpdateTimeBolt extends BaseRichBolt {
    public static final String UPDATE_TIME = "updateTime";
    private static Logger logger = LoggerFactory.getLogger(UpdateTimeBolt.class);
    private long now;
    private OutputCollector outputCollector;
    private int speed = 0;
    private long ptime = 0;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        speed++;
        if (speed == 1){
            ptime = System.currentTimeMillis();
        }
        if (logger.isInfoEnabled()){
            logger.info(format("updateTimeBolt received:%s,%s", tuple.getString(0), tuple.getLong(1)));
        }
        long time = tuple.getLong(1);
        if (now < time) {
            if (logger.isInfoEnabled()) {
                logger.info(format("[%s]:%s", UPDATE_TIME, tuple.toString()));
            }
            this.outputCollector.emit(UPDATE_TIME, new Values(time));
            now = time;
            if (logger.isInfoEnabled()){
                logger.info(format("updateTimeBolt sent:%s,%s", tuple.getString(0), tuple.getLong(1)));
            }
        } else {
            if (logger.isInfoEnabled()){
                logger.info(format("now >= time: %s >= %s", now, time));
            }
        }

        this.outputCollector.ack(tuple);

        if (speed % 1000 == 0){
            if (logger.isInfoEnabled()){
                logger.info(format("1000 cost time:%s", System.currentTimeMillis()-ptime));
            }
            speed = 0;
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(UPDATE_TIME, new Fields("time"));
    }
}
