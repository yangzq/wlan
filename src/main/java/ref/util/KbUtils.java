package ref.util;

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

    public boolean isInside(String loc, String cell) {
        return "tourist".equals(cell);  //默认支持wifi
    }

}
