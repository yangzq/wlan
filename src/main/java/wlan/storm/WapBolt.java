package wlan.storm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.commons.lang.StringUtils;
import wlan.util.EventTypeConst;

import java.net.SocketPermission;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: yangzq2
 * Date: 13-4-10
 * Time: 上午11:20
 */
public class WapBolt extends BaseBasicBolt {
    private BasicOutputCollector outputCollector;
    public static final String WAPSTREAM = "wapStream";
    private Map userMap = new HashMap<String, Set<Long>>();
    private final long timeInterval = 15 * 60 * 1000L;
    private long nowTime;

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        this.outputCollector = collector;
        if (input.getSourceStreamId().equals(PreconditionBolt.PRECONDITION)){
            String imsi = input.getString(0);
            String eventType = input.getString(1);
            long time = input.getLong(2);
            if (time < nowTime - timeInterval) return;
//            if (eventType.equals(EventTypeConst.EVENT_WAP_CONN) || eventType.equals(EventTypeConst.EVENT_WAP_USE)) {
            if (eventType.equals(EventTypeConst.EVENT_WAP_USE)) {
                HashSet<Long> timeUse = (HashSet<Long>) userMap.get(imsi);
                if (timeUse == null) {
                    timeUse = new HashSet<Long>();
                }
                timeUse.add(time);
                removeOutTime(timeUse, time);
                boolean match = checkUser(timeUse);
                if (match) {
                    userMap.remove(imsi);
                    collector.emit(WAPSTREAM, new Values(imsi, time));
                    try {
                        System.out.println(String.format("match: %s, time: %s", imsi, getTime(time)));
                    } catch (ParseException e) {
                        e.printStackTrace();
                    }
                } else {
                    userMap.put(imsi, timeUse);
                    System.out.println(String.format("add time: %s : %s", imsi, StringUtils.join(getTimeArr(timeUse.toArray()), ",")));
                }
            } else if (eventType.equals(EventTypeConst.EVENT_WAP_DISCONN)){
                userMap.remove(imsi);
                // 考虑乱序，只删除断开连接信令时间之前的信令
                try {
                    System.out.println(String.format("remove from userMap: %s, time: %s", imsi, getTime(time)));
                } catch (ParseException e) {
                    e.printStackTrace();
                }
            } else {
                // do nothing
            }
        } else if (input.getSourceStreamId().equals(PreconditionBolt.UPDATETIME)) {
            nowTime = input.getLong(0);
            updateGlobalTime(nowTime);
        }

    }

    private boolean checkUser(HashSet<Long> timeUse) {
        boolean result =  false;
        if (!(timeUse.size() < 20)){
            result = true;
        }
        return result;
    }

    private void removeOutTime(HashSet<Long> timeUseSet, long time) {
        Iterator iterator = timeUseSet.iterator();
        while (iterator.hasNext()){
            Long exTime = (Long)iterator.next();
            if (exTime < time - timeInterval) {
                iterator.remove();
            }
        }
    }

    private void updateGlobalTime(long time) {
        for (Object imsi: userMap.keySet()) {
            HashSet<Long> timeSet = (HashSet<Long>)userMap.get(imsi);
            removeOutTime(timeSet, time);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(WAPSTREAM, new Fields("imsi", "time"));
    }

    private static String getTime(long s) throws ParseException {
        return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(s - TimeZone.getDefault().getRawOffset()));
    }

    private String[] getTimeArr(Object[] mmArr){
        String[] fromatTime = new String[mmArr.length];
        for (int i = 0; i < mmArr.length; i++) {
            try {
                fromatTime[i] = getTime((Long)mmArr[i]);
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
        return fromatTime;
    }
}
