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
import wlan.util.TimeUtil;

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
    private Map userMap = new HashMap<String, UserData>();
    private final long timeInterval = 15 * 60 * 1000L;
    private long nowTime = 0L;
//    private long connTime = 0L;

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        this.outputCollector = collector;
        if (input.getSourceStreamId().equals(PreconditionBolt.PRECONDITION)) {
            String imsi = input.getString(0);
            String eventType = input.getString(1);
            long time = input.getLong(2);
            if (time < nowTime - timeInterval) return;
            if (eventType.equals(EventTypeConst.EVENT_WAP_CONN)) {
                if (userMap.containsKey(imsi)) {
                    userMap.remove(imsi);
                }
                UserData userData = new UserData(time, new HashSet<Long>());
                userMap.put(imsi, userData);
//                connTime = time;
            } else if (eventType.equals(EventTypeConst.EVENT_WAP_USE)) {
                UserData userData = (UserData) userMap.get(imsi);
                if (userData == null){
                    userData = new UserData(0, new HashSet<Long>());
                }
                if (userData.getTimeUseSet() == null) {
                    userData.setTimeUseSet(new HashSet<Long>());
                }
                if (userData.getConnTime() == 0L) {
                    userData.setConnTime(time);
                }
                userMap.put(imsi, userData);

                HashSet<Long> timeUseSet = (HashSet<Long>) userData.getTimeUseSet();
                timeUseSet.add(time);
                removeOutTime(timeUseSet, time);
                userData.setTimeUseSet(timeUseSet);
                boolean match = checkUser(userData, time);
                if (match) {
                    userMap.remove(imsi);
                    System.out.println(String.format("match: %s, signal time:%s; on time: %s/%s", imsi, StringUtils.join(getTimeArr(timeUseSet.toArray()), ","), TimeUtil.getTime(time), time));
                    outputCollector.emit(WAPSTREAM, new Values(imsi, time));
                } else {
                    userMap.put(imsi, userData);
                    System.out.println(String.format("add time: %s : %s; on time: %s/%s", imsi, StringUtils.join(getTimeArr(timeUseSet.toArray()), ","), TimeUtil.getTime(time), time));
                }
            } else if (eventType.equals(EventTypeConst.EVENT_WAP_DISCONN)) {
                userMap.remove(imsi);
                // 考虑乱序，只删除断开连接信令时间之前的信令
                    System.out.println(String.format("remove from userMap: %s, on time: %s/%s", imsi, TimeUtil.getTime(time), time));
            } else {
                // do nothing
            }
        } else if (input.getSourceStreamId().equals(PreconditionBolt.UPDATETIME)) {
            nowTime = input.getLong(0);
            updateGlobalTime(nowTime, input.getString(1));
        }

    }

    private boolean checkUser(UserData userData, long time) {
        boolean result = false;
        if (!(time < userData.getConnTime() + timeInterval)) {
            if (!(userData.getTimeUseSet().size() < 20)) {
                result = true;
            }
        }
        return result;
    }

    private void removeOutTime(HashSet<Long> timeUseSet, long time) {
        Iterator iterator = timeUseSet.iterator();
        while (iterator.hasNext()) {
            Long exTime = (Long) iterator.next();
            if (exTime < time - timeInterval) {
                iterator.remove();
            }
        }
    }

    private void updateGlobalTime(long time, String signalImsi) {
        for (Iterator iterator = userMap.entrySet().iterator(); iterator.hasNext(); ) {
            Map.Entry entry = (Map.Entry) iterator.next();
            String imsi = (String) entry.getKey();
            if (imsi.equals(signalImsi)) return; // 本人的修改全局时间信令不做处理
            UserData userData = (UserData) entry.getValue();
            HashSet<Long> timeSet = (HashSet<Long>) userData.getTimeUseSet();
            removeOutTime(timeSet, time);
            userData.setTimeUseSet(timeSet);
            userMap.put(imsi, userData);
            boolean matched = checkUser(userData, time);
            if (matched) {
                iterator.remove();
                outputCollector.emit(WAPSTREAM, new Values(imsi, time));
                    System.out.println(String.format("match: %s, signal time:%s; on time: %s/%s", imsi, StringUtils.join(getTimeArr(timeSet.toArray()), ","), TimeUtil.getTime(time), time));
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(WAPSTREAM, new Fields("imsi", "time"));
    }

    private String[] getTimeArr(Object[] mmArr) {
        String[] fromatTime = new String[mmArr.length];
        for (int i = 0; i < mmArr.length; i++) {
                fromatTime[i] = TimeUtil.getTime((Long) mmArr[i]);
        }
        return fromatTime;
    }

    public class UserData {
        private long connTime = 0L;
        private Set<Long> timeUseSet = new HashSet<Long>();

        public UserData(long connTime, Set<Long> timeUseSet) {
            this.connTime = connTime;
            this.timeUseSet = timeUseSet;
        }

        public long getConnTime() {
            return connTime;
        }

        public void setConnTime(long connTime) {
            this.connTime = connTime;
        }

        public Set<Long> getTimeUseSet() {
            return timeUseSet;
        }

        public void setTimeUseSet(Set<Long> timeUseSet) {
            this.timeUseSet = timeUseSet;
        }
    }
}
