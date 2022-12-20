package algorithms.common;

/*
This is an implementation of the Camera object described in the paper
"Constant-Time Snapshots with Applications to Concurrent Data Structures"
Yuanhao Wei, Naama Ben-David, Guy E. Blelloch, Panagiota Fatourou, Eric Ruppert, Yihan Sun
PPoPP 2021

Copyright (C) 2021 Yuanhao Wei

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

import jdk.internal.vm.annotation.Contended;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import main.support.ThreadID;

public class Camera {
  public static final int PADDING = 32;
  public static long[] dummyCounters = new long[ThreadID.MAX_THREADS*PADDING];
 @Contended
  public volatile long timestamp;

  public static boolean INCREMENT_ON_UPDATE = false;
  private static final AtomicLongFieldUpdater<Camera> timestampUpdater = AtomicLongFieldUpdater.newUpdater(Camera.class, "timestamp");
  private static final ThreadLocal<Integer> backoffAmount = new ThreadLocal<Integer>() {
      @Override
      protected Integer initialValue() {
          return 1;
      }
  };

  public static Camera camera = new Camera();

  public Camera() {
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
    // return timestampUpdater.getAndIncrement(camera);
    long ts = camera.timestamp;
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

  public static long getTimestamp() {
    return camera.timestamp;
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
