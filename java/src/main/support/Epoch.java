package main.support;

import jdk.internal.vm.annotation.Contended;

import main.support.ThreadID;
import main.support.Reclaimable;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.ArrayList;

public class Epoch<Node extends Reclaimable> {
  public static final int ANNOUNCES_BEFORE_COLLECT = 512;
  public static final int PADDING = 64;
  public static final int ACTIVATING = -1;
  public static final int INACTIVE = -2;
  public static final int MAX_THREADS = ThreadID.MAX_THREADS;

  public final AnnouncementArray announce;
//  public final int[] announce;
  public final int[] prevRetireEpoch;
  public final int[] announceCount;
  public final ArrayList<Node>[][] retiredNodes;
  @Contended
  public volatile int epochNum;
  private static final AtomicIntegerFieldUpdater<Epoch> epochUpdater = AtomicIntegerFieldUpdater.newUpdater(Epoch.class, "epochNum");

  public Epoch(){
      epochNum = 0;
      announce = new AnnouncementArray();
//      announce = new int[MAX_THREADS*PADDING];
      prevRetireEpoch = new int[(MAX_THREADS+1)*PADDING];
      announceCount = new int[(MAX_THREADS+1)*PADDING];
      retiredNodes = new ArrayList[3][(MAX_THREADS+1)*PADDING];
//      for(int i = 0; i < MAX_THREADS; i++) {
//          announce[i*PADDING] = INACTIVE;
//      }
  }

  public AnnouncementArray.Slot announceExtra() {
      AnnouncementArray.Slot slot = announce.getEmptySlot();
      int curEpoch = 0;
      do {
          curEpoch = epochNum;
          announce.announceExtra(slot, curEpoch);
      } while(curEpoch != epochNum);
      return slot;
  }

  public void unannounceExtra(AnnouncementArray.Slot slot) {
    announce.unannounceExtra(slot);
  }

  public void announce() {
      int curEpoch = 0;
      do {
          curEpoch = epochNum;
          announce.announce(curEpoch);
      } while(curEpoch != epochNum);

//      announce[idx] = curEpoch;
      tryAdvanceEpoch(curEpoch);

      // for(int i = prevAnn[idx]-1; i <= curEpoch-2 && i <= prevAnn[idx]+1; i++) {
      //   int rindex = (i+3)%3;
      //   if(retiredNodes[rindex][idx] != null) {
      //     for(int j = 0; j < retiredNodes[rindex][idx].size(); j++)
      //       retiredNodes[rindex][idx].get(j).reclaim();
      //     retiredNodes[rindex][idx].clear();
      //   }
      // }

      // curEpoch = epochNum;
      // prevAnn[idx] = curEpoch;
      // if(announce[idx] != curEpoch)
      //   announce[idx] = curEpoch;
  }

  public void tryAdvanceEpoch(int curEpoch) {
    int idx = (ThreadID.threadID.get()+1)*PADDING;
    int annCount = announceCount[idx];
    if(annCount == ANNOUNCES_BEFORE_COLLECT) {
      announceCount[idx] = 0;

      for(long ann : announce.scanAnnouncements()) {
          if(ann != INACTIVE && ann != curEpoch) return;
      }

      if(epochUpdater.compareAndSet(this, curEpoch, curEpoch+1)) { // garbage collect
          // int prevEpoch = (curEpoch+2)%3;
          // //System.out.println("epoch advanced");
          // for(int i = 0; i < MAX_THREADS*PADDING; i+=PADDING) {
          //     if(retiredNodes[prevEpoch][i] != null) {
          //         for(int j = 0; j < retiredNodes[prevEpoch][i].size(); j++)
          //             retiredNodes[prevEpoch][i].get(j).reclaim();
          //         retiredNodes[prevEpoch][i].clear();
          //     }
          // }
      }
    }
    else {
        announceCount[idx] = annCount+1;
    }
  }

  public void unannounce() {
//      int idx = (ThreadID.threadID.get()+1)*PADDING;
      announce.unannounce();
//      announce[idx] = INACTIVE;
      // prevAnnounce[idx] = prevAnnounce[idx] & ~1); // set inactive
      // Announce.set((ThreadID.threadID.get()+1)*PADDING, prevAnnounce.get());
  } 

  public void retire(Node node) {
    //System.out.println("This should not be happening");
    int idx = (ThreadID.threadID.get()+1)*PADDING;
    int curEpoch = epochNum;

    int prevEpoch = prevRetireEpoch[idx];
    for(int i = prevEpoch-1; i <= curEpoch-2 && i <= prevEpoch; i++) {
      int rindex = (i+3)%3;
      if(retiredNodes[rindex][idx] != null) {
        for(int j = 0; j < retiredNodes[rindex][idx].size(); j++)
          retiredNodes[rindex][idx].get(j).reclaim();
        retiredNodes[rindex][idx].clear();
      }
    }
    prevRetireEpoch[idx] = curEpoch;

    int bagIdx = curEpoch%3;
    if(retiredNodes[bagIdx][idx] == null)
        retiredNodes[bagIdx][idx] = new ArrayList(128);
    retiredNodes[bagIdx][idx].add(node);

    tryAdvanceEpoch(curEpoch);
  }

  public void reset() {
      epochNum = 0;
      for(int i = 0; i < MAX_THREADS*PADDING; i+=PADDING)
      {
        prevRetireEpoch[i] = 0;
        announceCount[i] = 0;
      }
      emptyRetiredLists();
      announce.reset();
  }

  public void emptyRetiredLists() {
    for(int i = 0; i < 3; i++) {
      for(int j = 0; j < MAX_THREADS*PADDING; j+=PADDING)
        if(retiredNodes[i][j] != null) {
          for(int k = 0; k < retiredNodes[i][j].size(); k++)
            retiredNodes[i][j].get(k).reclaim();
          retiredNodes[i][j].clear();
        }
    }
  }

  public int getNumRetired() {
    int sum = 0;
    for(int i = 0; i < 3; i++) {
      for(int j = 0; j < MAX_THREADS; j++)
        if(retiredNodes[i][j*PADDING] != null)
          sum += retiredNodes[i][j*PADDING].size();
    }
    return sum;
  }
}   