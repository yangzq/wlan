package ref.util;

import org.apache.commons.collections.Transformer;
import org.apache.commons.collections.map.LazyMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wlan.util.TimeUtil;

import java.io.Serializable;
import java.util.*;

import static java.lang.String.format;
import static wlan.util.TimeUtil.time2HHMM;
import static wlan.util.TimeUtil.time2HHMMSS;

/**
 * 针对某个指标判断用户是否是工作人员，比如连续5天白天在景区的人员。
 */
public class MetricsDetector implements Serializable {
    private static Logger logger = LoggerFactory.getLogger(MetricsDetector.class);
    private static final long ONE_DAY = 24 * 60 * 60 * 1000;
    private final Metrics metrics;
    private final LazyMap detectors = (LazyMap) LazyMap.decorate(new HashMap(), new DetectorTransformer());

    private class DetectorTransformer implements Transformer, Serializable {
        @Override
        public Object transform(final Object input) {
            return new DaysStayTimeDetector((String) input,metrics.name, metrics.startOfDay, metrics.endOfDay, metrics.stayTimeThreshold, new DaysStayTimeDetector.Listener() {
                @Override
                public void onChange(long startTime, long stayTime) {
                    MetricsDetector.this.onChange((String) input, stayTime, startTime);
                }
            });
        }
    }

    private final LazyMap workerDays = (LazyMap) LazyMap.decorate(new HashMap(), new WorkerDayTransformer());

    private class WorkerDayTransformer implements Transformer, Serializable {
        @Override
        public Object transform(final Object input) {
            return new HashSet<Long>();
        }
    }

    private long currentTime = -1;
    private Listener listener;

    private void onChange(String imsi, long stayTime, long startTime) {
        if (stayTime > metrics.stayTimeThreshold) {
            if (!((Set) workerDays.get(imsi)).contains(startTime)) {
                updateWorkerDay(ACTION.ADD, imsi, startTime, (Set) workerDays.get(imsi));
            }
        } else {
            if (((Set) workerDays.get(imsi)).contains(startTime)) {
                updateWorkerDay(ACTION.REMOVE, imsi, startTime, (Set) workerDays.get(imsi));
            }
        }
    }

    private void updateWorkerDay(ACTION action, String imsi, long startTime, Set days) {
        int oldSize = days.size();
        switch (action) {
            case ADD:
                days.add(startTime);
                break;
            case REMOVE:
                days.remove(startTime);
                break;
        }
        int newSize = days.size();

        if ((oldSize < metrics.daysThreshold ^ newSize < metrics.daysThreshold)
                || (oldSize == metrics.daysThreshold ^ newSize == metrics.daysThreshold)) {
            listener.onChange(imsi, newSize, metrics.daysThreshold);
        }
        if (logger.isInfoEnabled()) {
            if (logger.isDebugEnabled()) {
                StringBuilder sb = new StringBuilder();
                for (Object start : days) {
                    if (start instanceof Long) {
                        String time = TimeUtil.getTime(((Long) start).longValue());
                        time = time.substring(0, time.indexOf(" "));
                        sb.append(time);
                        sb.append(",");
                    }
                }
                logger.info(format("worker days change:imsi:[%s] in [%s~%s] get days:[%s]", imsi, time2HHMMSS(metrics.startOfDay), time2HHMMSS(metrics.endOfDay), sb.toString()));
            } else {
                logger.info(format("worker days change:imsi:[%s],days:[%d]", imsi, newSize));
            }
        }

    }

    private enum ACTION {ADD, REMOVE}

    public MetricsDetector(Listener listener, Metrics metrics) {
        this.metrics = metrics;
        this.listener = listener;
    }

    public void in(String imsi, long time) {
        DaysStayTimeDetector detector = (DaysStayTimeDetector) detectors.get(imsi);
        detector.in(time);
    }

    public void out(String imsi, long time) {
        DaysStayTimeDetector detector = (DaysStayTimeDetector) detectors.get(imsi);
        detector.out(time);
    }

    public void updateTime(long time) {
        if (time > currentTime) {
            for (Object detector : detectors.values()) {
                ((DaysStayTimeDetector) detector).update(time);
            }
            for (Object daysO : workerDays.entrySet()) {
                Set<Long> days = ((Map.Entry<String, Set<Long>>) daysO).getValue();
                if (days.size() >= metrics.daysThreshold) {
                    for (Iterator<Long> iterator = days.iterator(); iterator.hasNext(); ) {
                        Long next = iterator.next();
                        if (next - currentTime > metrics.daysThreshold * ONE_DAY) {
                            iterator.remove();
                        }
                    }
                    if (days.size() < metrics.daysThreshold) {
                        listener.onChange(((Map.Entry<String, Set<Long>>) daysO).getKey(), days.size(), metrics.daysThreshold);
                    }
                }
            }
            currentTime = time;
        }
    }

    public OrderedTimeWindow.Event<StayTimeDetector.Status> getLastEvent(String imsi) {
        DaysStayTimeDetector detector = (DaysStayTimeDetector) detectors.get(imsi);
        return detector.getLastEvent();
    }

    public static interface Listener {
        void onChange(String imsi, int days, int daysThreshold);
    }

    public boolean isWorker(long time, String imsi) {
        updateTime(time);
        Set days = (Set) workerDays.get(imsi);
        return days.size() >= 5;

    }

    public static class Metrics implements Serializable {
        private final long startOfDay;
        private final long endOfDay;
        private final long stayTimeThreshold;
        private final int daysThreshold;
        private final String name;

        public Metrics(long startOfDay, long endOfDay, long stayTimeThreshold, int daysThreshold) {
            this.startOfDay = startOfDay;
            this.endOfDay = endOfDay;
            this.stayTimeThreshold = stayTimeThreshold;
            this.daysThreshold = daysThreshold;
            this.name =  time2HHMM(this.startOfDay) + "~" + time2HHMM(this.endOfDay);
        }
    }
}
