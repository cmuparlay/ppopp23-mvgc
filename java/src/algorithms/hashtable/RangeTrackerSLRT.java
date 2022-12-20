package algorithms.hashtable;

import jdk.internal.vm.annotation.Contended;

import main.support.AnnScan;
import main.support.AnnouncementArray;
import main.support.Epoch;
import main.support.ThreadID;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReferenceArray;

public class RangeTrackerSLRT {
    public static class Range {
        final long low; final long high; final int idx;
        public Range(final int idx, final long low, final long high) {
            this.idx = idx;
            this.low = low;
            this.high = high;
        }
    }

    private static final int BATCH_SIZE = 4*ThreadID.MAX_THREADS; // TODO: tune parameter
    private static final int SECOND_POP_DELAY = 2;
    private static final int NUM_POPS = 1;

    private AnnouncementArray Ann = new AnnouncementArray();
//    private long[] Ann = new long[ThreadID.MAX_THREADS*Epoch.PADDING];
    private int[] secondPopDelay = new int[ThreadID.MAX_THREADS*Epoch.PADDING];
    
    @Contended
    private ConcurrentLinkedQueue<List<Range>> Q = new ConcurrentLinkedQueue<List<Range>>();
    private final List<Range>[] LDPools = (List<Range>[]) new List[ThreadID.MAX_THREADS*Epoch.PADDING];

//    public AnnScan[] sortedAnnouncements = new AnnScan[ThreadID.MAX_THREADS];
//    public int[] sortedAnnouncementsSize = new int[ThreadID.MAX_THREADS*Epoch.PADDING];
    @Contended
    public AtomicReferenceArray<AnnScan> globalAnnScan = new AtomicReferenceArray<>(Epoch.PADDING * 2 + 1);

    public RangeTrackerSLRT() {
        System.out.println("RANGE TRACKER SECOND_POP_DELAY: " + SECOND_POP_DELAY);
        System.out.println("RANGE TRACKER BATCH_SIZE: " + BATCH_SIZE);
        for(int i = 0; i < ThreadID.MAX_THREADS*Epoch.PADDING; i += Epoch.PADDING) {
            LDPools[i] = new ArrayList<Range>(BATCH_SIZE);
//            Ann[i] = invalid;
        }
        globalAnnScan.set(Epoch.PADDING, new AnnScan());
    }

    public long announce(final CameraSLRT camera) {
        long ts;
        do {
            ts = camera.timestamp;
            Ann.announce(ts);
        } while(ts != camera.timestamp);
        return ts;
    }

    public void unannounce() {
        Ann.unannounce();
//        Ann[ThreadID.threadID.get() * Epoch.PADDING] = invalid;
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

    private final AnnScan sortAnnouncements(final CameraSLRT camera, final long minScanTS) {
        AnnScan newScan = new AnnScan();
        for(int i = 0; i < 2; i++) {
            AnnScan oldScan = globalAnnScan.get(Epoch.PADDING);
            // if(oldScan.ts >= minScanTS) return oldScan;
            newScan.ts = camera.timestamp;
            newScan.size = Ann.scanAnnouncements(newScan.reserved, oldScan, newScan.ts);
            Arrays.sort(newScan.reserved, 0, newScan.size);
            if(globalAnnScan.compareAndSet(Epoch.PADDING, oldScan, newScan))
                return newScan;
        }
        return globalAnnScan.get(Epoch.PADDING);
    }

    private List<Range> intersect(final List<Range> MQ, final AnnScan ar, VcasHashtableSLRT ht) {
        int tid = ThreadID.threadID.get();
        int i = 0;
        final int initialCapacity = 4*BATCH_SIZE;
        ArrayList<Range> Needed = new ArrayList<>(initialCapacity);
        Set<Integer> Redudant = new HashSet<>(initialCapacity);
        for(Range r : MQ) {
            while(i < ar.size && ar.reserved[i] < r.high) i++;
            if(i == 0 || ar.reserved[i-1] < r.low) {
//                if(!Redudant.contains(r.idx)) {
                    Redudant.add(r.idx);
//                }
            }
            else Needed.add(r);
        }
        for(Integer idx : Redudant)
            ht.compact(idx);
//            ((VcasHashtableDiscSteam1.HeadNode)ht.buckets.get(idx)).compact(ar);

        return Needed;
    }

    public void deprecate(final CameraSLRT camera, final VcasHashtableSLRT ht, final int index, final long low, final long high) {
        int idx = ThreadID.threadID.get()*Epoch.PADDING;
        List<Range> ldpool = LDPools[idx];
        ldpool.add(new Range(index, low, high));
        if(ldpool.size() == BATCH_SIZE) {
            for(int ii = 0; ii < NUM_POPS; ii++) {
                List<Range> MQ = Q.poll();
                if(MQ != null) {
                    secondPopDelay[idx]++;
                    if(secondPopDelay[idx] == SECOND_POP_DELAY) {
                        final List<Range> list2 = Q.poll();
                        if(list2 != null) MQ = merge(MQ, list2);
                        secondPopDelay[idx] = 0;
                    } else {
                        // MQ = merge(MQ, ldpool);
                        // ldpool = new ArrayList<Range>(2*BATCH_SIZE);
                    }
                    MQ = merge(MQ, ldpool);
                    final AnnScan ar = sortAnnouncements(camera, MQ.get(MQ.size()-1).high);
                    final List<Range> Needed = intersect(MQ, ar, ht);
                    // ldpool = intersect(ldpool, ar, ht);
                    if(Needed.size() > 4*BATCH_SIZE) {
                        int size = Needed.size();
                        int size1 = size / 3;
                        int size2 = size / 3;
                        int size3 = size - size1 - size2;
                        ArrayList<Range> l1 = new ArrayList<>(size1);
                        ArrayList<Range> l2 = new ArrayList<>(size2);
                        ArrayList<Range> l3 = new ArrayList<>(size3);
                        for (int i = 0; i < size1; i++) l1.add(Needed.get(i));
                        for (int i = size1; i < size1 + size2; i++) l2.add(Needed.get(i));
                        for (int i = size1 + size2; i < size1 + size2 + size3; i++) l2.add(Needed.get(i));
                        Q.add(l1);
                        Q.add(l2);
                        Q.add(l3);
                    } else if(Needed.size() > 2*BATCH_SIZE) {
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
                    else ldpool = Needed;
                } else {
                    // final long[] ar = sortAnnouncements(camera);
                    // ldpool = intersect(ldpool, ar, ht);
                }
                if(ldpool.size() >= BATCH_SIZE/2) {
                   // ((ArrayList) ldpool).trimToSize();
                    Q.add(ldpool);
                    LDPools[idx] = new ArrayList<Range>(BATCH_SIZE);
                } else LDPools[idx] = ldpool;
            }
        }
    }

    public List<Range> ejectAll() {
        globalAnnScan.set(Epoch.PADDING, new AnnScan());
        List<Range> depreciatedObjects = new ArrayList<Range>();
        while(!Q.isEmpty()) {
            depreciatedObjects.addAll(Q.poll());
        }
        for(int i = 0; i < ThreadID.MAX_THREADS*Epoch.PADDING; i += Epoch.PADDING) {
            depreciatedObjects.addAll(LDPools[i]);
            LDPools[i] = new ArrayList<Range>(BATCH_SIZE);
        }
        System.out.println("Num Nodes in Range Tracker: " + depreciatedObjects.size());
        return depreciatedObjects;
    }
}
