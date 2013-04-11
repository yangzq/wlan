package ref.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ref.util.KbUtils;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

import static java.lang.String.format;

/**
 *
 */
public class TouristDetector implements MetricsDetector.Listener, Serializable {
    private static Logger logger = LoggerFactory.getLogger(TouristDetector.class);
    private final Set<String> tourists;
    private final Set<String> workers;
    private final MetricsDetector[] detectors;
    private final Listener listener;
    private long currentTime;

    public TouristDetector(Listener listener, MetricsDetector.Metrics... metricses) {
        detectors = new MetricsDetector[metricses.length];
        for (int i = 0; i < metricses.length; i++) {
            detectors[i] = new MetricsDetector(this, metricses[i]);
        }
        this.listener = listener;
        this.tourists = new HashSet<String>();
        this.workers = new HashSet<String>();
    }

    public void onSignaling(String imsi, long time, String loc, String cell) {
        boolean isInside = KbUtils.getInstance().isInside(loc, cell);
        for (MetricsDetector detector : detectors) {
            if (isInside) {
                detector.in(imsi, time);
            } else {
                detector.out(imsi, time);
            }
        }
        if (workers.contains(imsi)) {
            if (tourists.contains(imsi)) {
                tourists.remove(imsi);
                listener.removeTourist(imsi, time);
            }
        } else {
            OrderedTimeWindow.Event<StayTimeDetector.Status> lastEvent = null;
            for (MetricsDetector detector : detectors) {
                OrderedTimeWindow.Event<StayTimeDetector.Status> event = detector.getLastEvent(imsi);
                if (lastEvent == null || (event != null && lastEvent.time < event.time)) {
                    lastEvent = event;
                }
            }
            if (lastEvent != null && lastEvent.data == StayTimeDetector.Status.IN) {
                if (!workers.contains(imsi)) {
                    if (!tourists.contains(imsi)) {
                        tourists.add(imsi);
                        listener.addTourist(imsi, time);
                    }
                }
            } else {
                if (!workers.contains(imsi)) {
                    if (tourists.contains(imsi)) {
                        tourists.remove(imsi);
                        listener.removeTourist(imsi, time);
                    }
                }
            }
        }
    }

    @Override
    public void onChange(String imsi, int days, int daysThreshold) {
        if (days >= daysThreshold) {
            if (!workers.contains(imsi)) {
                workers.add(imsi);
                if (tourists.contains(imsi)) {
                    tourists.remove(imsi);
                    listener.removeTourist(imsi, currentTime);
                }
            }
        } else {//在一个 detector 里不是worker，可能在另一个 detector 里是worker
            for (MetricsDetector detector : detectors) {
                if (detector.isWorker(currentTime, imsi)) {
                    return;
                }
            }
            if (workers.contains(imsi)) {
                workers.remove(imsi);
            }
        }
        if (logger.isInfoEnabled()) {
            if (logger.isDebugEnabled()) {
                StringBuilder sb = new StringBuilder();
                for (String worker : workers) {
                    sb.append(worker);
                    sb.append(",");
                }
                logger.info(format("is worker change: imsi:[%s],days:[%d],works:[%s]", imsi, days, sb.toString()));
            } else {
                logger.info(format("is worker change: imsi:[%s],days:[%d]", imsi, days));
            }
        }

    }

    public void updateTime(long time) {
        if (currentTime < time) {
            for (MetricsDetector detector : detectors) {
                detector.updateTime(time);
            }
            currentTime = time;
        }
    }

    public static interface Listener {
        void addTourist(String imsi, long time);

        void removeTourist(String imsi, long time);
    }
}
