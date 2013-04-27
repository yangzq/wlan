package wlan.util;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

/**
 * Created with IntelliJ IDEA.
 * User: shaoaq
 * Date: 13-2-28
 * Time: 下午12:48
 *
 */
public class TimeUtil {
    private static final long ONE_SECOND = 1000;
    private static final long ONE_MINUTE = 60 * ONE_SECOND;
    private static final long ONE_HOUR = 60 * ONE_MINUTE;
    /**
     * 打印出零时区的时间： 0 -> 1970-01-01 00:00:00
     *
     * @param s
     * @return String "yyyy-MM-dd HH:mm:ss"
     */
    public static String getTime(long s) {
        return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date(s - TimeZone.getDefault().getRawOffset()));
    }

    public static void main(String[] args) {
        System.out.println(getTime(0));
    }

    public static String time2HHMMSS(long time) {
        StringBuilder sb = new StringBuilder();
        sb.append(longTo2c(time / ONE_HOUR));
        sb.append(":");
        sb.append(longTo2c((time % ONE_HOUR) / ONE_MINUTE));
        sb.append(":");
        sb.append(longTo2c((time % ONE_MINUTE) / ONE_SECOND));
        return sb.toString();
    }
    public static String time2HHMM(long time) {
        StringBuilder sb = new StringBuilder();
        sb.append(longTo2c(time / ONE_HOUR));
        sb.append(":");
        sb.append(longTo2c((time % ONE_HOUR) / ONE_MINUTE));
        return sb.toString();
    }

    public static String longTo2c(long l) {
        String s = Long.toString(l);
        return s.length() == 2 ? s : "0" + s;
    }
}
