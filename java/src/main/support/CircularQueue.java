package main.support;

import java.util.ArrayList;

public class CircularQueue<T> {
  private ArrayList<T> array;
  private int capacity;
  private int head; // current head
  private int tail; // index after current tail
  private boolean empty;

  public CircularQueue(int size) {
    capacity = size;
    array = new ArrayList<T>(size);
    for(int i = 0; i < size; i++)
      array.add(null);
    head = 0;
    tail = 0;
    empty = true;
  }

  public T enqueue(T obj) {
    if(tail == head && !empty) {
      T ret = array.get(tail);
      array.set(tail, obj);
      tail = increment(tail);
      head = tail;
      return ret;
    } else {
      array.set(tail, obj);
      tail = increment(tail);
      empty = false;
      return null;
    }
  }

  public T dequeue() {
    if(empty) return null;
    T ret = array.get(head);
    head = increment(head);
    if(head == tail) empty = true;
    return ret;
  }

  private int increment(int idx) {
    if(idx == capacity-1) return 0;
    else return idx+1;
  }
}