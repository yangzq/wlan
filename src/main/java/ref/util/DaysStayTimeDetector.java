package ref.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.String.format;

/**
 * 按天计算的停留时间。
 * 以18~8为例
 * 1. 使用一个StayTimeDetector来计算停留时间
 * 2. 每天晚上18点清空StayTimeDetector，开始重新计算（即新的统计周期开始时，清空上次的停留时间）
 * 3. 每天早上8点，更新时间，以便检测用户一直停留为离开的用户
 */
public class DaysStayTimeDetector implements StayTimeDetector.Listener {
    private static Logger logger = LoggerFactory.getLogger(DaysStayTimeDetector.class);
    private static long ONE_DAY = 24 * 60 * 60 * 1000;
    private final StayTimeDetector detector;
    private long startTime = -1;
    private String imsi;
    private long startOfDay; //相对当天的时间
    private long endOfDay; //相对当天的时间
    private final long stayTimeThreshold;
    private final Listener listener;
    private long stayTime;

    public DaysStayTimeDetector(String imsi, String metricsName, long startOfDay, long endOfDay, long stayTimeThreshold, Listener listener) {
        detector = new StayTimeDetector(imsi, metricsName, this);
        this.imsi = imsi;
        this.startOfDay = startOfDay;
        this.endOfDay = endOfDay;
        this.stayTimeThreshold = stayTimeThreshold;
        this.listener = listener;
    }

    public void in(long time) {
        update(time);
        detector.in(time);
    }

    public void out(long time) {
        update(time);
        detector.out(time);
    }

    public void update(long time) {
        detector.update(time);
        if (startTime < time && (startTime + ONE_DAY) > time) { //属于本次统计周期

        } else {
            startTime = getStartTime(time, startOfDay);

            detector.reset(startTime, this.startTime + this.endOfDay - this.startOfDay + ((this.endOfDay > this.startOfDay) ? 0 : ONE_DAY));
            this.stayTime = 0;
        }
    }

    private long getStartTime(long time, long startOfDay) {
        long timeOfDay = time % ONE_DAY;
        long days = time / ONE_DAY;
        return (timeOfDay >= startOfDay) ? (days * ONE_DAY + startOfDay) : (days * ONE_DAY + startOfDay - ONE_DAY);
    }

    public long getStayTime() {
        return stayTime;
    }

    @Override
    public void onChange(long stayTime) {
        if ((this.stayTime < stayTimeThreshold ^ stayTime < stayTimeThreshold)
                || (this.stayTime == stayTimeThreshold ^ stayTime == stayTimeThreshold)) {
            if (logger.isInfoEnabled()){
                logger.info(format("stay time change compared to threshold:[%d]", stayTime));
            }
            listener.onChange(startTime, stayTime);
        }
        this.stayTime = stayTime;
    }

    public OrderedTimeWindow.Event<StayTimeDetector.Status> getLastEvent() {
        return this.detector.getLastEvent();
    }

    public static interface Listener {
        void onChange(long startTime, long stayTime);
    }
}
