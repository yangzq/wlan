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

        byte[] input = new byte[]{48, 50, 50, 55, 48, 48, 77, 51, 48, 53, 48, 48, 57, 57, 57, 57, 57, 48, 50, 57, 57, 57, 57, 57, 57, 57, 57, 50, 48, 49, 51, 48, 52, 50, 53, 50, 48, 49, 51, 48, 49, 50, 52, 48, 49, 52, 50, 51, 48, 53, 55, 57, 53, 32, 32, 56, 56, 56, 56, 56, 48, 48, 48, 51, 53, 48, 50, 57, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 12, -86, -51, -105, -38, 0, 77, 90, 12, -86, -51, -105, -38, 0, 77, 90, 12, -86, -51, -105, -38, 0, 77, 90, 12, -86, -51, -105, -38, 0, 77, 90, 12, -86, -51, -105, -38, 0, 77, 90, 12, -86, -51, -105, -38, 0, 77, 90, 48, 48, 52, 51, 53, 54, 55, 54, 49, 53, 54, 50, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 49, 54, 56, 48, 51, 48, 52, 53, 49, 53, 49, 32, 32, 32, 32, 32, 32, 32, 32, 32, 48, 48, 49, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 68, 67, 68, 53, 51, 68, 70, 48, 57, 56, 49, 68, 50, 53, 65, 68};
        System.out.println(new String(input));
    }
}
