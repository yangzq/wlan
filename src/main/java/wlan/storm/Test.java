package wlan.storm;

import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: yangzq2
 * Date: 13-4-10
 * Time: 下午10:58
 */
public class Test {
    public static void main(String[] args) {
//        Set<Long> set = new HashSet<Long>();
//        set.add(100L);
//        set.add(200L);
//        set.add(300L);
//        Iterator iterator = set.iterator();
//        while (iterator.hasNext()){
//            Long currLong = (Long)iterator.next();
//            if (currLong == 400){
//                iterator.remove();
//            }
//        }
        Map<String, String> map = new HashMap<String, String>();
        map.put("1","aa");
        map.put("2","bb");
        map.put("3","cc");
        if (map.get("3").toString().equals("cc")){
            map.remove("3");
        }
        System.out.println();
    }
}
