package main.support;

public class ThreadID {
  public static final ThreadLocal<Integer> threadID = new ThreadLocal<Integer>();
  public static final int MAX_THREADS = (System.getenv().keySet().contains("MAX_THREADS") ? Integer.parseInt(System.getenv().get("MAX_THREADS")) : 141);
}