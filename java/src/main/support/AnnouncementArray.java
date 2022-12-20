package main.support;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLongArray;

public class AnnouncementArray {
    // TODO: make sure dynamically allocating slot objects isn't too slow
    public static class Slot {
        int idx;
        public Slot(int idx) {
            this.idx = idx;
        }
    }

    public static final int MAX_ANN = 64;
    private AtomicLongArray Ann; // TODO: does this need to be AtomicLong?
    private static final long INVALID = -3;

    public AnnouncementArray() {
        reset();
    }

    public void reset() {
        Ann = new AtomicLongArray(ThreadID.MAX_THREADS*MAX_ANN);
        for(int i = 0; i < ThreadID.MAX_THREADS*MAX_ANN; i += MAX_ANN) {
            Ann.set(i, 0);
            Ann.set(i+1, INVALID);
//            Ann[i] = 0; // represents the maximum number of announcements by this process
//            Ann[i+1] = INVALID;
        }
    }

    public long[] scanAnnouncements() {
        long[] announced = new long[ThreadID.MAX_THREADS + MAX_ANN];
        int counter = scanAnnouncements(announced);
        long[] ret = new long[counter];
        for(int i = 0; i < counter; i++) ret[i] = announced[i];
        return ret;
    }

    public int scanAnnouncements(long[] array) {
        int counter = 0;
        for(int tid = 0; tid < ThreadID.MAX_THREADS; tid++) {
            int max_ann = (int) Ann.get(tid*MAX_ANN);
            long ann = Ann.get(tid*MAX_ANN+1);
            if(ann != INVALID) {
                array[counter++] = ann;
            }
            for(int i = 2; i < max_ann+2; i++) {
                ann = Ann.get(tid*MAX_ANN+i);
                if(ann != INVALID) array[counter++] = ann;
            }
        }
        return counter;
    }

    public int scanAnnouncements(long[] array, AnnScan oldScan, long scanTS) {
        int counter = 0;
        for(int tid = 0; tid < ThreadID.MAX_THREADS; tid++) {
            int max_ann = (int) Ann.get(tid*MAX_ANN);
            long ann = Ann.get(tid*MAX_ANN+1);
            if(ann != INVALID && ann < scanTS) {
                if(oldScan == null || ann >= oldScan.ts || Arrays.binarySearch(oldScan.reserved, 0, oldScan.size, ann) >= 0)
                    array[counter++] = ann;
            }
            for(int i = 2; i < max_ann+2; i++) {
                ann = Ann.get(tid*MAX_ANN+i);
                if(ann != INVALID && ann < scanTS)
                    if(oldScan == null || ann >= oldScan.ts || Arrays.binarySearch(oldScan.reserved, 0, oldScan.size, ann) >= 0)
                        array[counter++] = ann;
            }
        }
        return counter;
    }

    public void announce(long ts) {
        int tid = ThreadID.threadID.get();
        Ann.set(tid*MAX_ANN+1, ts);
    }

    public void unannounce() {
        int tid = ThreadID.threadID.get();
        Ann.set(tid*MAX_ANN+1, INVALID);
    }

    public Slot getEmptySlot() {
        int tid = ThreadID.threadID.get();
        // TODO: implement faster way of finding empty slot
        int max_ann = (int) Ann.get(tid*MAX_ANN);
        for(int i = 2; i < max_ann+2; i++) {
            if(Ann.get(tid*MAX_ANN+i) == INVALID) {
//                Ann.set(tid*MAX_ANN+i, ts);
                return new Slot(i);
            }
        }
        // no empty slot was found, so increase max_ann
        max_ann++;
        Ann.set(tid*MAX_ANN, max_ann);
//        Ann.set(tid*MAX_ANN+max_ann+1, ts);
        return new Slot(max_ann+1);
    }

    // announces timestamp ts and returns the slot it was announced in
    public Slot announceExtra(long ts) {
        Slot slot = getEmptySlot();
        announceExtra(slot, ts);
        return slot;
    }

    public void announceExtra(Slot slot, long ts) {
        int tid = ThreadID.threadID.get();
        Ann.set(tid*MAX_ANN + slot.idx, ts);
    }

    public void unannounceExtra(Slot slot) {
        int tid = ThreadID.threadID.get();
        Ann.set(tid*MAX_ANN + slot.idx, INVALID);
    }

}
