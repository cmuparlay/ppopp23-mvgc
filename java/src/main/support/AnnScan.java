
package main.support;

import jdk.internal.vm.annotation.Contended;

public class AnnScan {
  @Contended("annscan group")
  public long ts;

  @Contended("annscan group")
  public int size;

  @Contended("annscan group")
  public long[] reserved;

  public AnnScan() {
    ts = -1;
    size = 0;
    reserved = new long[ThreadID.MAX_THREADS + AnnouncementArray.MAX_ANN]; // TODO: currently, this only lets one thread perform extra announcements
  }

  public static AnnScan[] annScan = new AnnScan[ThreadID.MAX_THREADS];

  public static AnnScan get() {
    int tid = ThreadID.threadID.get();
    // TODO: check if this is inefficient
    if(annScan[tid] == null)
      annScan[tid] = new AnnScan();
    return annScan[tid];
  }
}