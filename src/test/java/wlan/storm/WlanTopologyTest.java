package wlan.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import org.apache.mina.core.future.ConnectFuture;
import org.apache.mina.core.service.IoConnector;
import org.apache.mina.core.service.IoHandlerAdapter;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.filter.codec.textline.TextLineCodecFactory;
import org.apache.mina.transport.socket.nio.NioSocketConnector;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

/**
 * WlanTopology Tester
 */
public class WlanTopologyTest {


    @Before
    public void before() throws Exception {
    }

    @After
    public void after() throws Exception {

    }

    /**
     * Method: main(String[] args)
     */
    @Test
    public void testMain() throws Exception {
        TopologyBuilder builder = wlan.storm.WlanTopology.getTopologyBuilder();
        Config conf = new Config();
        conf.setDebug(true);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("wlan", conf, builder.createTopology());

        Thread.sleep(4 * 1000);
        Sender sender = new Sender(5002);

        sender.send("wlan 1", "02", "2013-01-04 08:01:00", "cause", "lac", "cell");
        sender.send("wlan 1", "03", "2013-01-04 08:01:30", "cause", "lac", "cell");
        sender.send("wlan 1", "03", "2013-01-04 08:02:00", "cause", "lac", "cell");
        sender.send("wlan 1", "03", "2013-01-04 08:02:30", "cause", "lac", "cell");
        sender.send("wlan 1", "03", "2013-01-04 08:03:00", "cause", "lac", "cell");
        sender.send("wlan 1", "03", "2013-01-04 08:03:30", "cause", "lac", "cell");
        sender.send("wlan 1", "03", "2013-01-04 08:04:00", "cause", "lac", "cell");
        sender.send("wlan 1", "03", "2013-01-04 08:04:30", "cause", "lac", "cell");
        sender.send("wlan 1", "03", "2013-01-04 08:05:00", "cause", "lac", "cell");
        sender.send("wlan 1", "03", "2013-01-04 08:05:30", "cause", "lac", "cell");
        sender.send("wlan 1", "03", "2013-01-04 08:06:00", "cause", "lac", "cell");
        sender.send("wlan 1", "03", "2013-01-04 08:06:30", "cause", "lac", "cell");
        sender.send("wlan 1", "03", "2013-01-04 08:07:00", "cause", "lac", "cell");
        sender.send("wlan 1", "03", "2013-01-04 08:07:30", "cause", "lac", "cell");
        sender.send("wlan 1", "03", "2013-01-04 08:08:00", "cause", "lac", "cell");
        sender.send("wlan 1", "03", "2013-01-04 08:08:30", "cause", "lac", "cell");
        sender.send("wlan 1", "03", "2013-01-04 08:09:00", "cause", "lac", "cell");
        sender.send("wlan 1", "03", "2013-01-04 08:09:30", "cause", "lac", "cell");
        sender.send("wlan 1", "03", "2013-01-04 08:10:00", "cause", "lac", "cell");
        sender.send("wlan 1", "03", "2013-01-04 08:10:30", "cause", "lac", "cell");

        sender.send("wlan 2", "03", "2013-01-04 08:12:00", "cause", "lac", "cell");

        sender.send("wlan 1", "04", "2013-01-04 08:30:00", "cause", "lac", "cell");

        Thread.sleep(1 * 1000);
        sender.close();
        Thread.sleep(1 * 1000);
        cluster.shutdown();
    }

    @Test
    public void testMain2() throws Exception {
        TopologyBuilder builder = wlan.storm.WlanTopology.getTopologyBuilder();
        Config conf = new Config();
        conf.setDebug(true);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("wlan", conf, builder.createTopology());

        Thread.sleep(4 * 1000);
        Sender sender = new Sender(5002);


        Thread.sleep(1 * 1000);
        sender.close();
        Thread.sleep(1 * 1000);
        cluster.shutdown();
    }

    @Test
    public void testMain3() throws Exception {
        TopologyBuilder builder = wlan.storm.WlanTopology.getTopologyBuilder();
        Config conf = new Config();
        conf.setDebug(true);
//        conf.put(Config.TOPOLOGY_DEBUG, true);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("wlan", conf, builder.createTopology());

        Thread.sleep(4 * 1000);
        Sender sender = new Sender(5002);
        BufferedReader reader = null;
        try {
//            String filePath = "/tmp/100001000002068.csv";
            String filePath = "/data.csv";
            reader = new BufferedReader(new InputStreamReader(getClass().getResourceAsStream(filePath)));
            for (String line = reader.readLine(); line != null; line = reader.readLine()) {
                String signal = line.substring(0, line.indexOf("calling") - 1);
                sender.send(signal);
                System.out.println("send:" + line);
            }
        } finally {
            if (reader != null) {
                reader.close();
            }
        }
        Thread.sleep(1000);
        sender.close();

        cluster.shutdown();
    }

    private static class Sender extends IoHandlerAdapter {
        private final IoConnector connector;
        private final IoSession session;

        private Sender(int port) {
            connector = new NioSocketConnector();
            connector.setHandler(this);
            connector.getFilterChain().addLast("codec", new ProtocolCodecFilter(new TextLineCodecFactory(Charset.forName("UTF-8"))));
            ConnectFuture future = connector.connect(new InetSocketAddress(port));
            future.awaitUninterruptibly();
            session = future.getSession();
        }

        @Override
        public void messageReceived(IoSession session, Object message) throws Exception {
            super.messageReceived(session, message);
        }

        public void send(String imsi, String event, String time, String cause, String loc, String cell) throws ParseException {
            StringBuilder sb = new StringBuilder();
            session.write(sb.append(imsi).append(",").append(event).append(",").append(getTime(time)).append(",").append(cause).append(",").append(loc).append(",").append(cell));
        }

        public void send(String imsi, String event, long time, String cause, String loc, String cell) throws ParseException {
            StringBuilder sb = new StringBuilder();
            session.write(sb.append(imsi).append(",").append(event).append(",").append(time).append(",").append(cause).append(",").append(loc).append(",").append(cell));
        }

        public void send(String signal) throws ParseException, InterruptedException {
            session.write(signal).await();
        }

        public void close() throws InterruptedException {
            session.close(false).await();
            connector.dispose();
        }
    }

    private static long getTime(String s) throws ParseException {
        return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss Z").parse(s + " +0000").getTime();
    }

    private static String getTime(long s) throws ParseException {
        return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(s - TimeZone.getDefault().getRawOffset()));
    }

    public static void main(String[] args) throws ParseException {
        System.out.println(getTime(1356999187197L));
        System.out.println(getTime(1357001569894L));
    }
}
