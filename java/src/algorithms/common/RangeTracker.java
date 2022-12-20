package algorithms.common;

import main.support.AnnouncementArray;
import main.support.Epoch;
import main.support.ThreadID;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

public class RangeTracker<T> {
    private static class Range {
        Object t; long low; long high;
        public Range(final Object t, final long low, final long high) {
            this.t = t;
            this.low = low;
            this.high = high;
        }
    }

    private static class ListPair {
        List<Object> Redundant = new ArrayList<Object>(4*BATCH_SIZE);
        List<Range> Needed = new ArrayList<Range>(4*BATCH_SIZE);
    }

    private static final long invalid = -3;
//    private static final int BATCH_SIZE = ThreadID.MAX_THREADS/2; // TODO: tune parameter
    private static final int BATCH_SIZE = ThreadID.MAX_THREADS*4; // TODO: tune parameter
    private static final int SECOND_POP_DELAY = 1;  // Note: previously 3
    
    private AnnouncementArray Ann = new AnnouncementArray();
//    private long[] Ann = new long[ThreadID.MAX_THREADS*Epoch.PADDING];
    private ConcurrentLinkedQueue<List<Range>> Q = new ConcurrentLinkedQueue<List<Range>>();
    private final List<Range>[] LDPools = (List<Range>[]) new List[ThreadID.MAX_THREADS*Epoch.PADDING];
    private int[] secondPopDelay = new int[ThreadID.MAX_THREADS*Epoch.PADDING];

    public RangeTracker() {
        for(int i = 0; i < ThreadID.MAX_THREADS*Epoch.PADDING; i += Epoch.PADDING) {
            LDPools[i] = new ArrayList<Range>(BATCH_SIZE);
        }
    }

    public long announce(final CameraBBF camera) {
        long ts;
        do {
            ts = camera.timestamp;
            Ann.announce(ts);
        } while(ts != camera.timestamp);
        return ts;
    }

    public void unannounce() {
        Ann.unannounce();
    }

    private List<Range> merge(final List<Range> l1, final List<Range> l2) {
        if(l1 == null) return l2;
        if(l2 == null) return l1;
        ArrayList<Range> merged = new ArrayList<>(l1.size() + l2.size());
        int i1 = 0, i2 = 0;
        while(i1 < l1.size() && i2 < l2.size()) {
            if(l1.get(i1).high < l2.get(i2).high) {
                merged.add(l1.get(i1));
                i1++;
            } else {
                merged.add(l2.get(i2));
                i2++;
            }
        }
        for(; i1 < l1.size(); i1++) merged.add(l1.get(i1));
        for(; i2 < l2.size(); i2++) merged.add(l2.get(i2));
        return merged;
    }

    private long[] sortAnnouncements() {
        long[] sortedAnn = Ann.scanAnnouncements();
//        for(int i = 0; i < ThreadID.MAX_THREADS; i++)
//            sortedAnn[i] = Ann[i * Epoch.PADDING];
        Arrays.sort(sortedAnn, 0, sortedAnn.length);
        return sortedAnn; // may contain 'invalid' but that's fine
    }

    private ListPair intersect(final List<Range> MQ, final long[] ar) {
        int i = 0;
        ListPair listPair = new ListPair();
        List<Object> Redundant = listPair.Redundant;
        List<Range> Needed = listPair.Needed;
        for(Range r : MQ) {
            while(i < ar.length && ar[i] < r.high) i++;
            if(i == 0 || ar[i-1] < r.low) Redundant.add(r.t);
            else Needed.add(r);
        }
        return listPair;
    }

    public List<Object> deprecate(final T o, final long low, final long high) {
        int idx = ThreadID.threadID.get()*Epoch.PADDING;
        List<Range> ldpool = LDPools[idx];
        ldpool.add(new Range(o, low, high));
        if(ldpool.size() == BATCH_SIZE) {
            List<Range> MQ = Q.poll();
            List<Object> Redundant = null;
            if(MQ != null) {
                secondPopDelay[idx]++;
                if(secondPopDelay[idx] == SECOND_POP_DELAY) {
                    final List<Range> list2 = Q.poll();
                    if(list2 != null) MQ = merge(MQ, list2);
                    secondPopDelay[idx] = 0;
                }
                long[] ar = sortAnnouncements();
                ListPair listPair = intersect(MQ, ar);
                ArrayList<Range> Needed = (ArrayList<Range>) listPair.Needed;
                Redundant = listPair.Redundant;
                // ldpool = intersect(ldpool, ar);
                if(Needed.size() > 2*BATCH_SIZE) {
		            int size = Needed.size();
                    ArrayList<Range> firstHalf = new ArrayList<>(size/2);
                    ArrayList<Range> secondHalf = new ArrayList<>(size - size/2);
                    for(int i = 0; i < size/2; i++) firstHalf.add(Needed.get(i));
                    for(int i = size/2; i < size; i++) secondHalf.add(Needed.get(i));
                    Q.add(firstHalf);
                    Q.add(secondHalf);
                } else if(Needed.size() > BATCH_SIZE) {
                    ((ArrayList) Needed).trimToSize();
                    Q.add(Needed);
                }
                else ldpool = merge(ldpool, Needed);
            } else {
                // final long[] ar = sortAnnouncements(camera);
                // ldpool = intersect(ldpool, ar);
            }
            // if(ldpool.size() > BATCH_SIZE/2) {
                Q.add(ldpool);
                LDPools[idx] = new ArrayList<Range>(BATCH_SIZE);
            // } else LDPools[idx] = ldpool;
            return Redundant;
        }
        return null;
    }
    
    public List<T> ejectAll() {
        List<Range> depreciatedObjects = new ArrayList<Range>();
        while(!Q.isEmpty()) {
            depreciatedObjects.addAll(Q.poll());
        }
        for(int i = 0; i < ThreadID.MAX_THREADS*Epoch.PADDING; i += Epoch.PADDING) {
            depreciatedObjects.addAll(LDPools[i]);
            LDPools[i] = new ArrayList<Range>(BATCH_SIZE);
        }
        List<T> depreciatedT = new ArrayList<T>();
        for(Range r : depreciatedObjects) depreciatedT.add((T) r.t);
        System.out.println("Num Nodes in Range Tracker: " + depreciatedObjects.size());
        return depreciatedT;
    }

    public List<T> ejectAllSafe() {
        List<Object> depreciatedObjects = new ArrayList<>();
        int neededNodes = 0;
        long ar[] = sortAnnouncements();
        while(!Q.isEmpty()) {
            ListPair pair = intersect(Q.poll(), ar);
            depreciatedObjects.addAll(pair.Redundant);
            neededNodes += pair.Needed.size();
        }
        for(int i = 0; i < ThreadID.MAX_THREADS*Epoch.PADDING; i += Epoch.PADDING) {
            ListPair pair = intersect(LDPools[i], ar);
            depreciatedObjects.addAll(pair.Redundant);
            neededNodes += pair.Needed.size();
            LDPools[i] = new ArrayList<Range>(BATCH_SIZE);
        }
        System.out.println("Num Nodes freed from Range Tracker: " + depreciatedObjects.size());
        System.out.println("Num Nodes still in Range Tracker: " + neededNodes);
        return (List<T>) depreciatedObjects;
    }
}
