package wlan.storm;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wlan.util.EventTypeConst;
import wlan.util.KbUtils;
import wlan.util.TimeUtil;

import java.util.*;


/**
 * Created with IntelliJ IDEA.
 * User: yangzq2
 * Date: 13-5-29
 * Time: 下午7:59
 */
public class WholeBolt extends BaseBasicBolt {
    private long lastSignalTime = 0L;
    private long nowTime = 0L;
    private final long timeInterval = 15 * 60 * 1000L;
    private Map userMap = new HashMap<String, UserData>();
    private Logger countLogger = LoggerFactory.getLogger("wlan.count");

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {

        String imsi = tuple.getString(0);
        String eventType = tuple.getString(1);
        Long time = tuple.getLong(2);
        String lac = tuple.getString(3);
        String cell = tuple.getString(4);
//        Values values = new Values(imsi, eventType, time, lac, cell);
        if (time > nowTime) nowTime = time;
        boolean wifi = KbUtils.getInstance().checkWifi(imsi);

        if (wifi){
            doWapBolt(imsi, eventType, time);
        } else {
        }

        if (time > lastSignalTime){
            updateGlobalTime(time, imsi);
            lastSignalTime = time;
        }

    }

    private void doWapBolt(String imsi1, String eventType1, Long time1){
        String imsi = imsi1;
        String eventType = eventType1;
        long time = time1;
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
                countLogger.info(String.format("%s:%s/%s", imsi, TimeUtil.getTime(time), time));
//                System.out.println(String.format("match: %s, signal time:%s; on time: %s/%s", imsi, StringUtils.join(getTimeArr(timeUseSet.toArray()), ","), TimeUtil.getTime(time), time));
//                outputCollector.emit(WAPSTREAM, new Values(imsi, time));
//                System.out.println(String.format("Send sms to:%s on signal time:%s/%s ", imsi, TimeUtil.getTime(time), time));
            } else {
                userMap.put(imsi, userData);
//                System.out.println(String.format("add time: %s : %s; on time: %s/%s", imsi, StringUtils.join(getTimeArr(timeUseSet.toArray()), ","), TimeUtil.getTime(time), time));
            }
        } else if (eventType.equals(EventTypeConst.EVENT_WAP_DISCONN)) {
            userMap.remove(imsi);
            // 考虑乱序，只删除断开连接信令时间之前的信令
//            System.out.println(String.format("remove from userMap: %s, on time: %s/%s", imsi, TimeUtil.getTime(time), time));
        } else {
            // do nothing
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
                countLogger.info(String.format("%s:%s/%s", imsi, TimeUtil.getTime(time), time));
//                outputCollector.emit(WAPSTREAM, new Values(imsi, time));
//                System.out.println(String.format("Send sms to:%s on signal time:%s/%s ", imsi, TimeUtil.getTime(time), time));
//                System.out.println(String.format("match: %s, signal time:%s; on time: %s/%s", imsi, StringUtils.join(getTimeArr(timeSet.toArray()), ","), TimeUtil.getTime(time), time));
            }
        }
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

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }
}
