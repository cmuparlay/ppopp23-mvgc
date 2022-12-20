package main.support;

import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

public class ConcurrentLinkedQueue<V> {
    static class Node {
        Object value;
        volatile Node next = null;
        public Node(Object value) { this.value = value; }
    }
    static private final AtomicReferenceFieldUpdater<Node, Node> nextUpdater = AtomicReferenceFieldUpdater.newUpdater(Node.class, Node.class, "next");
    static private final AtomicReferenceFieldUpdater<ConcurrentLinkedQueue, Node> headUpdater = AtomicReferenceFieldUpdater.newUpdater(ConcurrentLinkedQueue.class, Node.class, "head");
    static private final AtomicReferenceFieldUpdater<ConcurrentLinkedQueue, Node> tailUpdater = AtomicReferenceFieldUpdater.newUpdater(ConcurrentLinkedQueue.class, Node.class, "tail");

    private volatile Node head; // add padding between head and tail
    private volatile Node tail;

    public ConcurrentLinkedQueue() {
        head = new Node(null);
        tail = head;
    }

    public void add(V value) {
        Node node = new Node(value);
        while(true) {
            Node last = tail;
            Node next = last.next;
            if(last == tail) {
                if(next == null) {
                    if(nextUpdater.compareAndSet(last, next, node)) {
                        tailUpdater.compareAndSet(this, last, node);
                        return;
                    }
                } else {
                    tailUpdater.compareAndSet(this, last, next);
                }
            }
        }
    }

    public V poll() {
        while(true) {
            Node first = head;
            Node last = tail;
            Node next = first.next;
            if(first == head) {
                if(first == last) {
                    if(next == null) {
                        return null;
                    }
                    tailUpdater.compareAndSet(this, last, next);
                } else {
                    if(headUpdater.compareAndSet(this, first, next)) {
                        return (V) next.value;
                    }
                }
            }
        }
    }

    public boolean isEmpty() {
        return head == tail;
    }
}
