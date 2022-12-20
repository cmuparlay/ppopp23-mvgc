package algorithms.hashtable;

import algorithms.common.Camera;
import algorithms.common.CameraBBF;
import main.support.Epoch;
import main.support.Hash32;
import main.support.RangeScanResultHolder;
import main.support.ThreadID;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

public class VcasHashtableDLRT<K extends Comparable<? super K>,V> {

    private static final int DEFAULT_SIZE = 200000;

    private int num_buckets;
    public AtomicReferenceArray<HeadNode<K,V>> buckets;
    public static long[] nodesTraversedByRemove = new long[ThreadID.MAX_THREADS* Epoch.PADDING];
    public static int[] numRemoves = new int[ThreadID.MAX_THREADS* Epoch.PADDING];

    public static long[] nodesTraversedByReadVersion = new long[ThreadID.MAX_THREADS* Epoch.PADDING];
    public static int[] numReadVersions = new int[ThreadID.MAX_THREADS* Epoch.PADDING];

    private boolean isBaseline = false;
    public void setBaseline(boolean baseline) {
        isBaseline = baseline;
    }
    public boolean getBaseline() {
        return isBaseline;
    }

    public static class HeadNode<K extends Comparable<? super K>,V> {
        public K key;
        public V value;
        public Node<K,V> next;

        public volatile long ts;
        public volatile boolean marked; // TODO: combine marked with ts to save space
        public volatile HeadNode<K,V> leftv, rightv;

        private static final AtomicReferenceFieldUpdater<HeadNode, HeadNode> updateLeftv = AtomicReferenceFieldUpdater.newUpdater(HeadNode.class, HeadNode.class, "leftv");
        private static final AtomicReferenceFieldUpdater<HeadNode, HeadNode> updateRightv = AtomicReferenceFieldUpdater.newUpdater(HeadNode.class, HeadNode.class, "rightv");


        public static final long TBD = -1;
        public static final AtomicLongFieldUpdater<HeadNode> tsUpdater = AtomicLongFieldUpdater.newUpdater(HeadNode.class, "ts");
        public static final HeadNode dummyNextv = new HeadNode(null, null, null, 0, null);


        public HeadNode(K key, V value, Node next) {
            this(key, value, next, TBD, dummyNextv);
        }

        public HeadNode(K key, V value, Node next, long ts, HeadNode<K,V> nextv) {
            this.key = key;
            this.value = value;
            this.next = next;
            this.ts = ts;
            this.leftv = nextv;
            this.rightv = null;
            this.marked = false;
        }

        public void initTS() {
            if(ts == TBD) {
                long curTS = 0;
                if(Camera.INCREMENT_ON_UPDATE) curTS = CameraBBF.takeSnapshot()+1;
                else curTS = CameraBBF.getTimestamp();
                tsUpdater.compareAndSet(this, TBD, curTS);
            }
        }

        public void remove() {
            assert !this.marked;
            this.marked = true;
            // B.marked = true;
            HeadNode leftv = this.leftv;
            HeadNode rightv = this.rightv;
            HeadNode leftvRight = null, rightvLeft = null;
            int traversedNodes = 3; // counts number of marked nodes
            int tid = ThreadID.threadID.get();
            while(true) {
                while(leftv != null && leftv.marked) {
                    leftv = leftv.leftv;
                    traversedNodes++;
                }
                if(leftv != null) leftvRight = leftv.rightv;
                while(rightv != null && rightv.marked) {
                    rightv = rightv.rightv;
                    traversedNodes++;
                }
                if(rightv != null) rightvLeft = rightv.leftv;
                if((leftv != null && leftv.marked) || (rightv != null && rightv.marked)) continue;
                if(leftv != null && !updateRightv.compareAndSet(leftv, leftvRight, rightv)) continue;
                if(rightv != null && !updateLeftv.compareAndSet(rightv, rightvLeft, leftv)) continue;
                break;
            }
            nodesTraversedByRemove[tid*Epoch.PADDING] += traversedNodes;
            numRemoves[tid*Epoch.PADDING]++;
        }
    }

    public static class Node<K extends Comparable<? super K>,V> {
        public K key;
        public V value;
        public Node<K,V> next;

        public Node(K key, V value, Node next) {
            this.key = key;
            this.value = value;
            this.next = next;
        }
    }

    public boolean tryAppend(int idx, final HeadNode B, final HeadNode C) {
        if(B != null) {
            HeadNode A = B.leftv;
            if(A != null && A.rightv == null) HeadNode.updateRightv.compareAndSet(A, null, B);
        }
        HeadNode.updateLeftv.compareAndSet(C, HeadNode.dummyNextv, B);
        if(buckets.compareAndSet(idx, B, C)) {
            if(B != null) HeadNode.updateRightv.compareAndSet(B, null, C);
            return true;
        } else return false;
    }

    // requires that buckets.get(idx) != null
    public HeadNode<K,V> bucketGet(int idx) {
        HeadNode<K,V> head = buckets.get(idx);
        head.initTS();
        return head;
    }

    public HeadNode<K,V> bucketGet(int idx, long ts) {
        int traversedNodes = 1;
        int tid = ThreadID.threadID.get();

        HeadNode<K,V> head = buckets.get(idx);
        head.initTS();
//        System.out.println(head.ts);
        assert head.ts != HeadNode.TBD;
        assert head.leftv != HeadNode.dummyNextv;
        while(head != null && head.ts > ts) {
            head = head.leftv;
            traversedNodes++;
            assert head == null || head.ts != HeadNode.TBD;
            assert head == null || head.leftv != HeadNode.dummyNextv;
//            System.out.println("head.ts: " + head.ts);
        }

        nodesTraversedByReadVersion[tid*Epoch.PADDING] += traversedNodes;
        numReadVersions[tid*Epoch.PADDING]++;

        return head;
    }

    public void bucketInit(int idx, HeadNode<K,V> head) {
        boolean result = bucketCompareAndSet(idx, null, head);
        assert result;
    }

    public boolean bucketCompareAndSet(int idx, HeadNode<K,V> oldHead, HeadNode<K,V> newHead) {
        HeadNode<K,V> head = buckets.get(idx);
        if(head != null) head.initTS();
        if(head != oldHead) return false;
        if(oldHead == newHead) return true;
        if(tryAppend(idx, oldHead, newHead)) {
            newHead.initTS();
//            System.out.println(newHead.ts);
            if(oldHead != null) {
                long headts = oldHead.ts;
                long newts = newHead.ts;
                if(headts == newts) head.remove();
                else {
                    List<HeadNode> Redundant = CameraBBF.rt.deprecate(head, headts, newts);
                    if(Redundant != null)
                        for(HeadNode node : Redundant) {
                            // System.out.println("Node returned by range tracker");
                            node.remove();
                        }
                }
            }
            return true;
        } else {
            head = buckets.get(idx);
            head.initTS();
            return false;
        }
    }

    /**
     * Only call if there are no ongoing operations. Clears the RangeTracker data structure and calls remove() on
     * all the nodes returned
     */
    public static void cleanup() {
        long counter = 0;
        for(HeadNode node : (List<HeadNode>) CameraBBF.rt.ejectAll()) {
            node.remove();
            counter++;
        }
        System.out.println("Cleaned up " + counter + " nodes");
    }

    public void compactAll() {
        for(HeadNode node : (List<HeadNode>) CameraBBF.rt.ejectAllSafe())
            node.remove();
    }
    
    public VcasHashtableDLRT() {
        this(DEFAULT_SIZE);
    }

    public VcasHashtableDLRT(int size) {
//        System.out.println("Size: " + size);
        num_buckets = size;
        buckets = new AtomicReferenceArray<>(num_buckets);
        for(int i = 0; i < num_buckets; i++)
            bucketInit(i, new HeadNode<>(null, null, null));
//            buckets.set(i, new HeadNode<>(null, null, null));
    }

    public int hash(K key) {
        return (Hash32.hash((Integer) key) & 0x7fffffff);
    }

    public int hash(Integer key) {
        return (Hash32.hash((Integer) key) & 0x7fffffff);
    }

    public V get(K key) {
        int idx = hash(key) % num_buckets;
        return get(bucketGet(idx), (Integer) key);
    }

    public V get(Integer key) {
        int idx = hash(key) % num_buckets;
        return get(bucketGet(idx), (Integer) key);
    }

    public V getVersion(Integer key, long ts) {
        int idx = hash(key) % num_buckets;
        return get(bucketGet(idx, ts), key);
    }

    // TODO: replace Integer with K
    private V get(HeadNode<K,V> head, Integer key) {
        if(head.key == null || ((Integer)(head.key)).compareTo(key) > 0) return null;
        if(((Integer)(head.key)).compareTo(key) == 0) return head.value;
        Node<K,V> node = head.next;
        while(node != null && ((Integer)(node.key)).compareTo(key) < 0)
            node = node.next;
        if(node != null && ((Integer)(node.key)).compareTo(key) == 0) return node.value;
        else return null;
    }

    public boolean containsKey(K key) {
        return get(key) != null;
    }

    // copy everything from [start, end), make last node point to next
    // assumes start != null
    public Node<K,V> pathCopy(Node<K,V> start, Node<K,V> end, Node<K,V> next) {
        if(start == end) return next;
        return new Node<K,V>(start.key, start.value, pathCopy(start.next, end, next));
    }

    public boolean putIfAbsent(K key, V val) {
        int idx = hash(key) % num_buckets;
        while (true) {
            HeadNode<K,V> oldHead = bucketGet(idx);
            HeadNode<K,V> newHead = putIfAbsent(oldHead, key, val);
            if(oldHead == newHead) return false;
            if(bucketCompareAndSet(idx, oldHead, newHead))
                return true;
        }
    }

    private HeadNode<K,V> putIfAbsent(HeadNode<K,V> head, K key, V value) {
        if(head.key == null)
            return new HeadNode<K,V>(key, value, null);
        if(head.key.compareTo(key) > 0)
            return new HeadNode<>(key, value, new Node(head.key, head.value, head.next));
        if(head.key.compareTo(key) == 0) return head;
        Node<K,V> node = head.next;
        while(node != null && node.key.compareTo(key) < 0)
            node = node.next;
        if(node != null && node.key.compareTo(key) == 0) return head;
        Node<K,V> newNode = new Node<>(key, value, node);
        return new HeadNode<>(head.key, head.value, pathCopy(head.next, node, newNode));
    }

    public boolean remove(K key) {
        int idx = hash(key) % num_buckets;
        while (true) {
            HeadNode<K,V> oldHead = bucketGet(idx);
            HeadNode<K,V> newHead = remove(oldHead, key);
            if(oldHead == newHead) return false;
            if(bucketCompareAndSet(idx, oldHead, newHead))
                return true;
        }
    }

    private HeadNode<K,V> remove(HeadNode<K,V> head, K key) {
        if(head.key == null || head.key.compareTo(key) > 0)
            return head;
        if(head.key.compareTo(key) == 0) {
            Node<K,V> next = head.next;
            if(next == null) return new HeadNode<K,V>(null, null, null);
            return new HeadNode<K,V>(next.key, next.value, next.next);
        }
        Node<K,V> node = head.next;
        while(node != null && node.key.compareTo(key) < 0)
            node = node.next;
        if(node != null && node.key.compareTo(key) == 0)
            return new HeadNode<>(head.key, head.value, pathCopy(head.next, node, node.next));
        return head;
    }

    // Reference to a thread local variable that is used by
    // RangeScan to return the result of a range query
    private final ThreadLocal<RangeScanResultHolder> rangeScanResult = new ThreadLocal<RangeScanResultHolder>() {
        @Override
        protected RangeScanResultHolder initialValue() {
            return new RangeScanResultHolder();
        }
    };
    
    public Object[] rangeScan(final K a, final K b) {
        RangeScanResultHolder rangeScanResultHolder = rangeScanResult.get();
        rangeScanResultHolder.rsResult.clear();

        long ts = 0;
        if(Camera.INCREMENT_ON_UPDATE) ts = CameraBBF.getTimestamp();
        else ts = CameraBBF.takeSnapshot();
//        System.out.println(ts);

        for(int i = (Integer) a; i <= (Integer) b; i++) {
            V value = getVersion(i, ts);
//            System.out.println(i + " " + getVersion(i, ts));
            if(value != null)
                rangeScanResultHolder.rsResult.push(i);
        }

        if(!isBaseline) CameraBBF.unreserve();

        // Get stack and its number of elements
        Object[] stackArray = rangeScanResultHolder.rsResult.getStackArray();
        int stackSize = rangeScanResultHolder.rsResult.getEffectiveSize();

        // Make a copy of the stack and return it
        Object[] returnArray = new Object[stackSize];
        for (int i = 0; i < stackSize; i++)
            returnArray[i] = stackArray[i];
        return returnArray;
    }

    public Object[] rangeScanNonAtomic(final K a, final K b) {
        RangeScanResultHolder rangeScanResultHolder = rangeScanResult.get();
        rangeScanResultHolder.rsResult.clear();

//        System.out.println(ts);

        for(int i = (Integer) a; i <= (Integer) b; i++) {
            V value = get(i);
//            System.out.println(i + " " + getVersion(i, ts));
            if(value != null)
                rangeScanResultHolder.rsResult.push(value);
        }

        // Get stack and its number of elements
        Object[] stackArray = rangeScanResultHolder.rsResult.getStackArray();
        int stackSize = rangeScanResultHolder.rsResult.getEffectiveSize();

        // Make a copy of the stack and return it
        Object[] returnArray = new Object[stackSize];
        for (int i = 0; i < stackSize; i++)
            returnArray[i] = stackArray[i];
        return returnArray;
    }

    public long sequentialSize() {
        long size = 0;
        for(int i = 0; i < num_buckets; i++) size += sequentialSize(bucketGet(i));
        return size;
    }

    private long sequentialSize(HeadNode<K,V> head) {
        if(head.key == null) return 0;
        return 1 + sequentialSize(head.next);
    }

    private long sequentialSize(Node<K,V> n) {
        if(n == null) return 0;
        else return 1 + sequentialSize(n.next);
    }

    public long getKeysum() {
        long sum = 0;
        for(int i = 0; i < num_buckets; i++) sum += getKeysum(bucketGet(i));
        return sum;
    }

    private long getKeysum(HeadNode<K,V> head) {
        if(head.key == null) return 0;
        return (Integer) head.key + getKeysum(head.next);
    }

    private long getKeysum(Node<K,V> n) {
        if(n == null) return 0;
        else return (Integer) n.key + getKeysum(n.next);
    }

    @Override
    public String toString() {
        String s = "Hashtable: " + num_buckets + " buckets\n";
        for(int i = 0; i < num_buckets; i++) {
            String tmp = toString(bucketGet(i));
            if(tmp != "") s += tmp + '\n';
        }
        return s;
    }

    public String toString(long ts) {
        String s = "Hashtable: " + num_buckets + " buckets\n";
        for(int i = 0; i < num_buckets; i++) {
            String tmp = toString(bucketGet(i, ts));
            if(tmp != "") s += tmp + '\n';
        }
        return s;
    }

    private String toString(HeadNode<K,V> head) {
        if(head.key == null) return "";
        return head.key + " " + toString(head.next);
    }

    public String toString(Node<K,V> n) {
        if(n == null) return "";
        return n.key + " " + toString(n.next);
    }
    
    public ArrayList<Integer> versionListLengths() {
        ArrayList<Integer> lengths = new ArrayList<>();
        for(int i = 0; i < num_buckets; i++)
            if(bucketGet(i) != null)
                lengths.add(versionListLength(bucketGet(i)));
        return lengths;
    }

    private int versionListLength(HeadNode node) {
        int count = 0;
        while(node != null) {
            count++;
            node = node.leftv;
        }
        return count;
    }

    public ArrayList<Integer> countNodes(boolean traverseVersionList) {
        ArrayList<Integer> lengths = new ArrayList<>();
        HashSet<Node> visited = new HashSet<Node>();
        for(int i = 0; i < num_buckets; i++)
            if(bucketGet(i) != null)
                lengths.add(versionListNodes(bucketGet(i), visited, traverseVersionList));
        return lengths;
    }

    private int versionListNodes(HeadNode node, HashSet<Node> visited, boolean traverseVersionList) {
        if(node == null) return 0;
        int count = 1 + versionListNodes(node.next, visited);
        if(traverseVersionList) {
            node = node.leftv;
            while(node != null) {
                count += 1 + versionListNodes(node.next, visited);
                node = node.leftv;
            }
        }
        return count;
    }

    private int versionListNodes(Node node, HashSet<Node> visited) {
        int count = 0;
        while(node != null && !visited.contains(node)) {
            count++;
            visited.add(node);
            node = node.next;
        }
        return count;
    }

    public double avgNodesTraversedDuringRemove() {
        long nodesTraversedByRemoveSum = 0;
        long numRemovesSum = 0;
        for(int i = 0; i < ThreadID.MAX_THREADS; i++) {
            nodesTraversedByRemoveSum += nodesTraversedByRemove[i*Epoch.PADDING];
            numRemovesSum += numRemoves[i*Epoch.PADDING];
        }
        return 1.0*nodesTraversedByRemoveSum / numRemovesSum;
    }

    public double avgNodesTraversedDuringReadVersion() {
        long nodesTraversedByReadVersionSum = 0;
        long numReadVersionsSum = 0;
        for(int i = 0; i < ThreadID.MAX_THREADS; i++) {
            nodesTraversedByReadVersionSum += nodesTraversedByReadVersion[i*Epoch.PADDING];
            numReadVersionsSum += numReadVersions[i*Epoch.PADDING];
        }
        return 1.0*nodesTraversedByReadVersionSum / numReadVersionsSum;
    }

    public void resetStats() {
        for(int i = 0; i < ThreadID.MAX_THREADS; i++) {
            nodesTraversedByRemove[i*Epoch.PADDING] = 0;
            numRemoves[i*Epoch.PADDING] = 0;
            nodesTraversedByReadVersion[i*Epoch.PADDING] = 0;
            numReadVersions[i*Epoch.PADDING] = 0;
        }
    }
}