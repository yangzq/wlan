package ref.util;


import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 *
 */
public class OrderedTimeWindow<T> {
    private Listener listener;
    private long size;
    private long slotSize;
    private Circular<Slot<Event<T>>> slots;
    private Event<T> lastEvent = null; //保存当前slot之外的第一个event。如果插入到slot最前面，这个event就是pre

    public OrderedTimeWindow(Listener<T> listener, long size, long slotSize) {
        this.listener = listener;
        this.size = size;
        this.slotSize = slotSize;
        //保证当slot切换，即有一个slot里没有值时，剩余的slot里能存的下整个slotSize
        int slotCount = (size % slotSize == 0) ? ((int) (size / slotSize) + 1) : ((int) (size / slotSize) + 2);
        this.slots = new Circular<Slot<Event<T>>>(slotCount);
    }

    public void add(long time, T data) {
        Event<T> current = new Event<T>(time, data);
        Event<T> pre = null;
        List<Event<T>> nexts = new ArrayList<Event<T>>();
        for (int i = 0; i < slots.size(); i++) {
            Slot<Event<T>> slot = slots.get(i);
            if (slot == null) { //如果没有初始化，则初始化
                for (int j = slots.size() - 1; j >= 0; j--) {
                    slot = new Slot<Event<T>>(time - slotSize * j, time - slotSize * j + slotSize);
                    addSlot(slot);
                }
                slot.list.add(current);
                this.listener.onInsert(pre, current, new Event[0]);
                break;
            } else {
                if (slot.startTime <= time && slot.endTime > time) { //找到所属的slot
                    boolean findPosition = false;
                    int j = slot.list.size() - 1;
                    for (; j >= 0; j--) {//从后面开始查找插入位置
                        pre = slot.list.get(j);
                        if (pre.time <= time) {  //找到插入位置
                            findPosition = true;
                            break;
                        }
                    }
                    if (!findPosition) {//如果没找到，则是这个slot里的第一个
                        pre = getLastEvent(i); //前面的元素则是从上一个slot开始最近一个Event
                    }
                    slot.list.add(j + 1, current);
                    Collections.reverse(nexts);
                    this.listener.onInsert(pre, current, nexts.toArray(new Event[nexts.size()]));
                    break;
                } else if (i == 0 && slot.endTime <= time) { //如果是大于最新的slot的时间，则新增slot
                    while (time >= slot.endTime) {
                        Slot<Event<T>> newSlot = new Slot<Event<T>>(slot.endTime, slot.endTime + slotSize);
                        addSlot(newSlot);
                        slot = newSlot;
                    }
                    slot.list.add(current);
                    pre = getLastEvent(i);
                    this.listener.onInsert(pre, current, new Event[0]);
                    break;
                } else { //如果不属于当前slot，则将当前slot里的所有event加入到nexts，并继续找下一个
                    for (int j = slot.list.size() - 1; j >= 0; j--) {//从后面开始加入到nexts
                        nexts.add(slot.list.get(j));
                    }
                    continue;
                }
            }
        }
    }

    /**
     * 查找i之前的最后一个Event
     *
     * @param i
     * @return
     */
    public Event<T> getLastEvent(int i) {
        Event<T> pre;
        pre = null;
        for (int j = i + 1; j < slots.size(); j++) {
            Slot<Event<T>> lastSlot = slots.get(j);
            if (lastSlot.list.size() > 0) {
                pre = lastSlot.list.get(lastSlot.list.size() - 1);
                break;
            }
        }
        if (pre == null) {
            pre = lastEvent;
        }
        return pre;
    }

    public void update(long time) {
        //可以清空endTime< time - size 之前的slot,以提高内存使用率。也可以什么都不做
        //因为大部分情况下都不会引起slot切换，小部分的也只是切换一个slot，所以我们这里只需要看看最后一个slot
        Slot<Event<T>> latest = slots.get(slots.size());
        if (latest != null && latest.endTime < time - size) {
            Slot<Event<T>> newest = slots.get(0);
            Slot<Event<T>> slot = new Slot<Event<T>>(newest.endTime, newest.endTime + slotSize);
            addSlot(slot);
        }
    }

    private void addSlot(Slot<Event<T>> slot) {
        Slot<Event<T>> oldSlot = slots.add(slot);
        if (oldSlot != null && oldSlot.list.size() > 0) {
            lastEvent = oldSlot.list.get(oldSlot.list.size() - 1);
        }
    }

    private static class Slot<T> {
        private long startTime;
        private long endTime;
        private List<T> list = new ArrayList<T>();

        private Slot(long startTime, long endTime) {
            this.startTime = startTime;
            this.endTime = endTime;
        }
    }


    public static interface Listener<T> {
        void onInsert(Event<T> pre, Event<T> currrent, Event<T>[] nexts);

    }

    public static class Event<T> {
        public long time;
        public T data;

        public Event(long time, T data) {
            this.time = time;
            this.data = data;
        }
    }
}
