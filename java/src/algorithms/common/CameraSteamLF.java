package algorithms.common;

import jdk.internal.vm.annotation.Contended;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReferenceArray;

import main.support.AnnouncementArray;
import main.support.Epoch;
import main.support.ThreadID;
import main.support.AnnScan;

public class CameraSteamLF {
  public static final int PADDING = 32;
  public static AnnouncementArray announcement = new AnnouncementArray();
//  public static long[] announcement = new long[ThreadID.MAX_THREADS*PADDING];
  public static long[] dummyCounters = new long[ThreadID.MAX_THREADS*PADDING];
 @Contended
  public volatile long timestamp;

  private static final AtomicLongFieldUpdater<CameraSteamLF> timestampUpdater = AtomicLongFieldUpdater.newUpdater(CameraSteamLF.class, "timestamp");
  private static final ThreadLocal<Integer> backoffAmount = new ThreadLocal<Integer>() {
      @Override
      protected Integer initialValue() {
          return 1;
      }
  };

  public static CameraSteamLF camera = new CameraSteamLF();
   @Contended
  public static AtomicReferenceArray<AnnScan> globalAnnScan = new AtomicReferenceArray<>(Epoch.PADDING * 2 + 1);

  public CameraSteamLF() {
    timestamp = 0;
  }

  private static void backoff(int amount) {
      if(amount == 0) return;
      int limit = amount;
      int tid = ThreadID.threadID.get();
      for(int i = 0; i < limit; i++)
          dummyCounters[tid*PADDING] += i; 
  }

  public static void set(long ts) {
    camera.timestamp = ts;
  }

  public static long takeSnapshot() {
    long ts = -1, ts2 = camera.timestamp;
    if(Camera.INCREMENT_ON_UPDATE) ts = ts2;
    else {
      while(ts != ts2) {
        ts = ts2;
        // you may see a timestamp that was not actually announced
        announcement.announce(ts);
//      announcement[tid*PADDING] = ts;
        ts2 = camera.timestamp;
      }
    }

    int ba = backoffAmount.get();
    //if(ba != 1) System.out.println(ba);
    backoff(ba);
    if(ts == camera.timestamp) {
      if(timestampUpdater.compareAndSet(camera, ts, ts+1))
        ba /= 2;
      else
        ba *= 2;
    }
    if(ba < 1) ba = 1;
    if(ba > 512) ba = 512;
    backoffAmount.set(ba);
    return ts;
  }

  public static void scanAnnouncements(AnnScan scan) {
    AnnScan newScan = new AnnScan();
    for(int i = 0; i < 2; i++) {
      AnnScan oldScan = globalAnnScan.get(Epoch.PADDING);
      newScan.ts = camera.timestamp;
      newScan.size = announcement.scanAnnouncements(newScan.reserved, oldScan, newScan.ts);
      Arrays.sort(newScan.reserved, 0, newScan.size);
      if(globalAnnScan.compareAndSet(Epoch.PADDING, oldScan, newScan)) {
        scan.ts = newScan.ts;
        scan.size = newScan.size;
        scan.reserved = newScan.reserved;
        return;
      }
    }
    newScan = globalAnnScan.get(Epoch.PADDING);
    scan.ts = newScan.ts;
    scan.size = newScan.size;
    scan.reserved = newScan.reserved;
  }

  public static void unreserve() {
    announcement.unannounce();
  }

  public static long getTimestamp() {
    if(Camera.INCREMENT_ON_UPDATE) {
      long ts = -1, ts2 = camera.timestamp;
      while(ts != ts2) {
        ts = ts2;
        // you may see a timestamp that was not actually announced
        announcement.announce(ts);
        ts2 = camera.timestamp;
      }
      return ts;
    }
    else return camera.timestamp;
  }
}

// import jdk.internal.vm.annotation.Contended;
// import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
// import main.support.ThreadID;
// import main.support.Random;

// public class Camera {
//   @Contended
//   public volatile int timestamp;

//   public static final int MAX_THREADS = 150;
//   public static final int PADDING = 64;
//   public long[] backoffArray;

//   public static ThreadLocal<Random> rng = new ThreadLocal();

//   private static final AtomicIntegerFieldUpdater<Camera> timestampUpdater = AtomicIntegerFieldUpdater.newUpdater(Camera.class, "timestamp");
//   public static Camera camera = new Camera();

//   public Camera() {
//     timestamp = 0;
//     backoffArray = new long[(MAX_THREADS+1)*PADDING];
//   }

//   public static int takeSnapshot() {
//     // backoff
//     int ts = camera.timestamp;
//     int backoffAmount = rng.get().nextNatural(200);
//     // if(backoffAmount < 5) return timestampUpdater.getAndIncrement(camera);
//     for(int i = 0; i < backoffAmount; i++) {
//       camera.backoffArray[(ThreadID.threadID.get()+1)*PADDING]++;
//       if(i % 20 == 0 && ts != camera.timestamp)
//         return ts;
//     }
//     if(ts == camera.timestamp) {
//       return timestampUpdater.getAndIncrement(camera);
//     } else {
//       return ts;
//     }
//     // int counter = 0;
//     // while(camera.timestamp == ts) {
//     //   counter++;
//     //   // if(counter == 100000)
//     //     // System.out.println("counter limit reached");
//     // }
//     // return ts;
//     // // int ts = camera.timestamp;
//     // // timestampUpdater.compareAndSet(camera, ts, ts+1);
//     // // return ts;
//   }

//   public static int getTimestamp() {
//     return camera.timestamp;
//   }
// }

// import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
// import jdk.internal.vm.annotation.Contended;

// public class Camera {
//   @Contended
//   public volatile int ts1;
//   @Contended
//   public volatile int ts2;
//   @Contended
//   public volatile int ts3;
//   @Contended
//   public volatile int ts4;

//   private static final AtomicIntegerFieldUpdater<Camera> ts1Updater = AtomicIntegerFieldUpdater.newUpdater(Camera.class, "ts1");
//   private static final AtomicIntegerFieldUpdater<Camera> ts2Updater = AtomicIntegerFieldUpdater.newUpdater(Camera.class, "ts2");
//   private static final AtomicIntegerFieldUpdater<Camera> ts3Updater = AtomicIntegerFieldUpdater.newUpdater(Camera.class, "ts3");
//   private static final AtomicIntegerFieldUpdater<Camera> ts4Updater = AtomicIntegerFieldUpdater.newUpdater(Camera.class, "ts4");

//   private static Camera camera = new Camera();
//   private static final int NUM_TS = 4;

//   private static final ThreadLocal<Integer> tsIndex = new ThreadLocal<Integer>() {
//         @Override
//         protected Integer initialValue() {
//             return new Integer(0);
//         }
//     };

//   public Camera() {
//     ts1 = 0;
//     ts2 = 0;
//     ts3 = 0;
//     ts4 = 0;
//   }

//   public static int takeSnapshot() {
//     int ret = getTimestamp();
//     int index = tsIndex.get().intValue() + 1;
//     tsIndex.set(index % 1);
//     if(index == 1) {
//       ts1Updater.getAndIncrement(camera);
//     } else if(index == 2) {
//       ts2Updater.getAndIncrement(camera);
//     } else if(index == 3) {
//       ts3Updater.getAndIncrement(camera);
//     } else if(index == 4) {
//       ts4Updater.getAndIncrement(camera);
//     } else {
//       System.out.println("index is too large");
//     }
//     return ret;
//     // int ts = camera.timestamp;
//     // timestampUpdater.compareAndSet(camera, ts, ts+1);
//     // return ts;
//   }

//   public static int getTimestamp() {
//     return camera.ts1 + camera.ts2 + camera.ts3 + camera.ts4;
//   }
// }

// import java.util.concurrent.atomic.AtomicInteger;

// public class Camera {
//   private static AtomicInteger timestamp = new AtomicInteger(0);
//   public static int takeSnapshot() {
//     return timestamp.getAndIncrement();
//     // int ts = timestamp.getAndIncrement();
//     // int ts = timestamp.get();
//     // if(ts == timestamp.get())
//     //   timestamp.incrementAndGet();
//     // return ts;
//   }
//   public static int getTimestamp() {
//     return timestamp.get();
//   }
// }
