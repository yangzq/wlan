package ref.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.String.format;
import static wlan.util.TimeUtil.getTime;
import static wlan.util.TimeUtil.time2HHMMSS;

/**
 * 针对特定用户，检测停留时间。
 * 当停留时间变化时时，回调处理函数。
 * <p/>
 * 窗口保存最近10分钟的数据，当窗口数据变化时，重新计算停留时间
 */
public class StayTimeDetector implements OrderedTimeWindow.Listener<StayTimeDetector.Status> {
    private static Logger logger = LoggerFactory.getLogger(StayTimeDetector.class);
    private static Logger statyTimelogger = LoggerFactory.getLogger("tourist.stayTime");
    private static final long ONE_SECOND = 1000;
    private static final long ONE_MINUTE = 60 * ONE_SECOND;
    private static final long ONE_HOUR = 60 * ONE_MINUTE;

    private long stayTime;
    private OrderedTimeWindow orderedTimeWindow = new OrderedTimeWindow(this, 13 * ONE_MINUTE, 2 * ONE_MINUTE);
    private long startTime;
    private long endTime;
    private String metricsName;
    private String imsi;
    private Listener listener;

    public StayTimeDetector(String imsi, String metricsName, Listener listener) {
        this.metricsName = metricsName;
        this.imsi = imsi;
        this.listener = listener;
    }

    /**
     * 当新的统计周期开始时调用这个方法。
     *
     * @param startTime
     */
    public void reset(long startTime, long endTime) {
        if (startTime != this.startTime) {
            if(statyTimelogger.isDebugEnabled()){
                statyTimelogger.debug(format("%s reset from %d to %d", imsi, this.startTime, startTime));
            }
            this.startTime = startTime;
            this.endTime = endTime;
            this.stayTime = 0;
        }
    }

    public void in(long time) {
        orderedTimeWindow.add(time, Status.IN);
    }

    public void out(long time) {
        orderedTimeWindow.add(time, Status.OUT);
    }

    public void update(long time) {
        orderedTimeWindow.update(time);
    }

    @Override
    public void onInsert(OrderedTimeWindow.Event<Status> pre, OrderedTimeWindow.Event<Status> currrent, OrderedTimeWindow.Event<Status>[] nexts) {
        OrderedTimeWindow.Event<Status>[] oldEvents = new OrderedTimeWindow.Event[nexts.length + 1];
        oldEvents[0] = pre;
        System.arraycopy(nexts, 0, oldEvents, 1, nexts.length);
        for (int i = oldEvents.length - 1; i > 0; i--) {
            rollback(oldEvents[i - 1], oldEvents[i]);
        }
        OrderedTimeWindow.Event<Status>[] newEvents = new OrderedTimeWindow.Event[nexts.length + 2];
        newEvents[0] = pre;
        newEvents[1] = currrent;
        System.arraycopy(nexts, 0, newEvents, 2, nexts.length);
        for (int i = 0; i < newEvents.length - 1; i++) {
            append(newEvents[i], newEvents[i + 1]);
        }
    }

    private long backToRange(long time) {
        return max(min(this.endTime, time), this.startTime);
    }

    private void append(OrderedTimeWindow.Event<Status> pre, OrderedTimeWindow.Event<Status> current) {
        if (pre != null && pre.data == Status.IN) { //如果上次在里面，则累加停留时间。其他情况下停留时间不变。
            updateStayTime(backToRange(current.time) - backToRange(pre.time), pre.time, current.time);
        }
    }

    private void rollback(OrderedTimeWindow.Event<Status> pre, OrderedTimeWindow.Event<Status> current) {
        if (pre == null) {//pre==null,说明第一次插入了一个Event1，第二次插入了一个Event2，且Event2在Event1前面。这种情况下不需要回退
        } else {
            if (pre.data == Status.IN) { //如果上次在里面，则回退停留时间。其他情况下停留时间不变。
                updateStayTime(-(backToRange(current.time) - backToRange(pre.time)), pre.time, current.time);
            }
        }
    }


    private void updateStayTime(long delta, long pre, long current) {
        if (delta != 0) {
            stayTime += delta;
            if (logger.isInfoEnabled()) {
                if (logger.isDebugEnabled()) {
                    logger.debug(format("[%s~%s] update stay time:[%s] <-- [%s~%s] to [%s]", getTime(this.startTime), getTime(this.endTime), time2HHMMSS(delta), getTime(pre), getTime(current), time2HHMMSS(this.stayTime)));
                } else {
                    logger.info(format("update stay time:[%d]", delta));
                }
            }
            if (statyTimelogger.isInfoEnabled()) {
                try {
                    statyTimelogger.info(format("%s,%s,%d,%d,%d,[%s]", imsi, metricsName, current, stayTime, delta, getFormatTime(current)));
                } catch (ParseException e) {
                    e.printStackTrace();
                }
            }
            this.listener.onChange(stayTime);
        }
    }


    public OrderedTimeWindow.Event<Status> getLastEvent() {
        return this.orderedTimeWindow.getLastEvent(-1);
    }

    public static interface Listener {
        void onChange(long stayTime);
    }

    public static enum Status {
        IN, OUT
    }

    String getFormatTime(long s) throws ParseException {
        return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date(s- TimeZone.getDefault().getRawOffset()));
    }
}
