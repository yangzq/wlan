package wlan.util;

/**
 * 事件类型常量定义
 * @author yangzq2
 *
 */
public class EventTypeConst {
    public static final String EVENT_MMS_MO = "00"; // 彩信MO
    public static final String EVENT_MMS_MT = "01"; // 彩信MT
    public static final String EVENT_WAP_CONN = "02"; // WAP 连接（Connect消息）
    public static final String EVENT_WAP_USE = "03"; // WAP使用（WAP GET;WAP POST消息）
    public static final String EVENT_WAP_DISCONN = "04"; // WAP断开（WAP Disconnect消息）
    public static final String EVENT_OTHERS = "99"; // 其他事件
}