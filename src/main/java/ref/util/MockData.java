package ref.util;

import java.io.File;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: shaoaq
 * Date: 13-1-24
 * Time: 上午9:29
 *
 */
public class MockData {
    private static final SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static final SimpleDateFormat fileNameFormat = new SimpleDateFormat("yyyyMMddHHmmss");

    public static void main1(String[] args) throws ParseException {
        List<Generator> generators = new ArrayList<Generator>();
        generate(generators, "2013-01-01 00:00:00", "2013-02-10 00:00:00", "E:/out");
    }

    private static void generate(List<Generator> generators, String fromStr, String toStr, String outDirStr) throws ParseException {
        Calendar now = Calendar.getInstance();
        now.setTime(format.parse(fromStr));
        Calendar to = Calendar.getInstance();
        to.setTime(format.parse(toStr));
        File outDir = new File(outDirStr);
        outDir.mkdirs();
        while (now.before(to)) {
            List<Record> records = new ArrayList<Record>();
            for (Generator generator : generators) {
                records.addAll(generator.generate(now, 1));
            }
            sortWithLittleOutOfOrder(records);
        }
    }

    private static class Record {
        private Calendar time;
        private String content;

        private Record(Calendar time, String content) {
            this.time = time;
            this.content = content;
        }
    }

    public static void main(String[] args) {
        List<Record> records = new ArrayList<Record>();
        for (int i = 100; i > 0; i--) {
            Calendar time = Calendar.getInstance();
            time.setTimeInMillis(i * 1000);
            records.add(new Record(time, "" + i));
        }
        sortWithLittleOutOfOrder(records);
        for (Record r : records) {
            System.out.println(r.content);
        }
    }

    private static void sortWithLittleOutOfOrder(List<Record> records) {
        Object[] a = records.toArray();
        Arrays.sort(a, (Comparator) new Comparator<Record>() {
            @Override
            public int compare(Record o1, Record o2) {
                return o1.time.compareTo(o2.time);
            }
        });
        Random random = new Random();
        for (int i = 0; i < a.length - 1; i++) {
            Record r1 = (Record) a[i];
            Record r2 = (Record) a[i + 1];
            if ((r2.time.getTimeInMillis() / 1000 - r1.time.getTimeInMillis() / 1000 < 20)
                    && (random.nextInt() % 50 == 0)) {  //20秒以内有1/50的几率乱序
                System.out.println(r1.content);
                a[i] = r2;
                a[i + 1] = r1;
            }
        }
        ListIterator i = records.listIterator();
        for (int j = 0; j < a.length; j++) {
            i.next();
            i.set(a[j]);
        }
    }

    private static interface Generator {
        Collection<? extends Record> generate(Calendar now, int seconds);
    }

    private static class TouristGenerator implements Generator {

        @Override
        public Collection<? extends Record> generate(Calendar now, int seconds) {
            return null;
        }
    }

    private static class ScenicWorkerGenerator implements Generator {

        @Override
        public Collection<? extends Record> generate(Calendar now, int seconds) {
            return null;
        }
    }

    private static class NormalGenerator implements Generator {

        @Override
        public Collection<? extends Record> generate(Calendar now, int seconds) {
            return null;
        }
    }
}
