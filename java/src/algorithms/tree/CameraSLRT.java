package algorithms.tree;

import algorithms.common.Camera;
import jdk.internal.vm.annotation.Contended;

import main.support.ThreadID;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;

public class CameraSLRT {
  public static final int PADDING = 32;
  public static RangeTrackerSLRT rt = new RangeTrackerSLRT();
  public static long[] dummyCounters = new long[ThreadID.MAX_THREADS*PADDING];
 @Contended
  public volatile long timestamp;

  private static final AtomicLongFieldUpdater<CameraSLRT> timestampUpdater = AtomicLongFieldUpdater.newUpdater(CameraSLRT.class, "timestamp");
  private static final ThreadLocal<Integer> backoffAmount = new ThreadLocal<Integer>() {
      @Override
      protected Integer initialValue() {
          return 1;
      }
  };

  public static CameraSLRT camera = new CameraSLRT();

  public CameraSLRT() {
    timestamp = 0;
  }

  private static void backoff(final int amount) {
      if(amount == 0) return;
      int limit = amount;
      int tid = ThreadID.threadID.get();
      for(int i = 0; i < limit; i++)
          dummyCounters[tid*PADDING] += i; 
  }

  public static void set(final long ts) {
    camera.timestamp = ts;
  }

  public static long takeSnapshot() {
    // return timestampUpdater.getAndIncrement(camera);
    long ts = 0;
    if(Camera.INCREMENT_ON_UPDATE) ts = camera.timestamp;
    else ts = rt.announce(camera);

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

  public static void unreserve() {
    rt.unannounce();
  }

  public static long getTimestamp() {
    if(Camera.INCREMENT_ON_UPDATE) return rt.announce(camera);
    else return camera.timestamp;
  }
}
