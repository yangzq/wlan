package wlan.storm;

import org.apache.commons.lang.StringUtils;

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
        System.out.println(map);

//        String cont = map.get("2");
        String cont = "abcd";
        map.put("2", cont);
//        for (Iterator iterator = map.entrySet().iterator(); iterator.hasNext(); ){
//            Map.Entry entry = (Map.Entry) iterator.next();
//            String imsi = (String) entry.getKey();
//            String cont = (String) entry.getValue();
////            System.out.println(imsi + ":" + cont);
//            if (imsi.equals("2")){
//                cont="abc";
//                map.put(imsi, cont);
//            }
//        }
        System.out.println(map);
//        if (map.get("3").toString().equals("cc")){
//            map.remove("3");
//        }
//        String line = "100001000002068,02,1357891261000,cause,xc,home,calling,called,apn,sgsnIp,res2,2013-01-11 08:01:01.000";
//        String signal = line.substring(0, line.indexOf("calling") - 1);
//        System.out.println(signal);

//        ArrayList<Long> arrayList = new ArrayList<Long>();
//        arrayList.add(100L);
//        arrayList.add(200L);
//        arrayList.add(300L);
//        arrayList.add(120L);
//
//        System.out.println(StringUtils.join(arrayList, ","));
        System.out.println();
    }
}
