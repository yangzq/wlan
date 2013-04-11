package wlan.util;

/**
 *
 */
public class KbUtils {
    static class SingletonHolder {
        static KbUtils instance = new KbUtils();
    }

    public static KbUtils getInstance() {
        return SingletonHolder.instance;
    }

    /**
     * 检查手机终端是否支持wifi，暂时设定为全部返回支持
     * @param imsi
     * @return
     */
    public boolean checkWifi(String imsi){
        return true;
    }

}
