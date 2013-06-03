package wlan.storm;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wlan.util.NioServer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

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
    private BufferedReader reader;

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
        queue = new LinkedBlockingQueue<String>(500);

        new Thread() {
            @Override
            public void run() {
                try {
                    ServerSocket serverSocket = new ServerSocket(port);
                    while (true) {
                        Socket socket = serverSocket.accept();
                        reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    }
                } catch (IOException e) {
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                }
            }
        }.start();


//        NioServer.Listener listener = new NioServer.Listener() {
//            @Override
//            public void messageReceived(String message) throws Exception {
//                if (logger.isDebugEnabled()) {
//                    logger.info(String.format("spout received:%s", message));
//                }
//                queue.put(message); // 往队列中添加信令时阻塞以保证数据不丢失
//            }
//        };
//        nioServer = new NioServer(port, listener);
//        try {
//            nioServer.start();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }

    }

    private long count = 0;
    private long start = 0;

    @Override
    public void nextTuple() {
        int count =0;
        if (reader != null) {
            try {
                for (String line = reader.readLine(); line != null; line = reader.readLine()) {
                    String[] columns = line.split(",");
                    if(columns.length>5){
                        Values tuple = new Values(columns[0], columns[1], Long.parseLong(columns[2]), columns[4], columns[5]);
                        spoutOutputCollector.emit(SIGNALLING, tuple);
                    }
                    if(count++>1000){
                        break;
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
        }
////        Utils.sleep(10);
//        String message = null;
//        int i = 0;
//        List<Object> list = new ArrayList<Object>();
//        while ((message = queue.poll()) != null) {
////            if (++i % 2000 == 0)
////                Utils.sleep(10);
//            String[] columns = message.split(",");
//            Values tuple = new Values(columns[0], columns[1], Long.parseLong(columns[2]), columns[4], columns[5]);
//            list.add(tuple);
//            if (logger.isDebugEnabled()) {
//                logger.debug(format("[%s]:%s", SIGNALLING, tuple.toString()));
//            }
//            if(++i%5000==0){
////                spoutOutputCollector.emitDirect();
//                spoutOutputCollector.emit(SIGNALLING,list,null);
//            }
////            spoutOutputCollector.emit(SIGNALLING, tuple);
//            if (logger.isDebugEnabled()) {
//                logger.info(String.format("spout sent:%s,%s,%s", tuple.get(0), tuple.get(1), tuple.get(2)));
//            }
//            if(count%100000==0){
//                start=System.currentTimeMillis();
//            }else if(count%100000==(100000-1)){
//                new File("/home/stream_dev/tmp","spout_"+((int)(System.currentTimeMillis()-start)/1000)).mkdir();
//            }
//            count++;
//        }
//        spoutOutputCollector.emit(SIGNALLING,list,null);
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
