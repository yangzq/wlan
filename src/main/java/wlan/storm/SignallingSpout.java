package wlan.storm;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import backtype.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wlan.util.NioServer;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import static java.lang.String.format;

/**
 *
 */
public class SignallingSpout extends BaseRichSpout {
    public static final String SIGNALLING = "signalStream";
    private static Logger logger = LoggerFactory.getLogger(SignallingSpout.class);
    LinkedBlockingQueue<String> queue = null;
    NioServer nioServer = null;
    private SpoutOutputCollector spoutOutputCollector;
    private int port;

    public SignallingSpout(int vport) {
        this.port = vport;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(SIGNALLING, new Fields("imsi", "eventType", "time", "lac", "cell"));
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector = spoutOutputCollector;
        queue = new LinkedBlockingQueue<String>(10000);
        NioServer.Listener listener = new NioServer.Listener() {
            @Override
            public void messageReceived(String message) throws Exception {
                if (logger.isDebugEnabled()) {
                    logger.info(String.format("spout received:%s", message));
                }
                queue.put(message); // 往队列中添加信令时阻塞以保证数据不丢失
            }
        };
        nioServer = new NioServer(port, listener);
        try {
            nioServer.start();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void nextTuple() {
        Utils.sleep(10);
        String message = null;
        int i = 0;
        while ((message = queue.poll()) != null) {
            if (++i % 2000 == 0)
                Utils.sleep(10);
            String[] columns = message.split(",");
            Values tuple = new Values(columns[0], columns[1], Long.parseLong(columns[2]), columns[4], columns[5]);
            if (logger.isDebugEnabled()) {
                logger.debug(format("[%s]:%s", SIGNALLING, tuple.toString()));
            }
            spoutOutputCollector.emit(SIGNALLING, tuple);
            if (logger.isDebugEnabled()) {
                logger.info(String.format("spout sent:%s,%s,%s", tuple.get(0), tuple.get(1), tuple.get(2)));
            }
        }
    }

    @Override
    public void close() {
        try {
            nioServer.stop();
        } catch (IOException e) {
            if (logger.isWarnEnabled()) {
                logger.warn("error when stop nioServer", e);
            }
        }
    }

    @Override
    public void ack(Object msgId) {
        super.ack(msgId);
        if (logger.isDebugEnabled()) {
            logger.debug("successfully ack(): " + msgId.toString());
        }
    }

    @Override
    public void fail(Object msgId) {
        super.fail(msgId);
        if (logger.isErrorEnabled()) {
            logger.error("fail(): " + msgId.toString());
        }
    }
}
