package algorithms.common;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

public class DoublyLinkedListBBF {
    public static final AtomicReferenceFieldUpdater<Node, Node> updateLeft = AtomicReferenceFieldUpdater.newUpdater(Node.class, Node.class, "left");
    public static final AtomicReferenceFieldUpdater<Node, Node> updateRight = AtomicReferenceFieldUpdater.newUpdater(Node.class, Node.class, "right");

    public static final AtomicReferenceFieldUpdater<Node, Descriptor> updateLeftDesc = AtomicReferenceFieldUpdater.newUpdater(Node.class, Descriptor.class, "leftDesc");
    public static final AtomicReferenceFieldUpdater<Node, Descriptor> updateRightDesc = AtomicReferenceFieldUpdater.newUpdater(Node.class, Descriptor.class, "rightDesc");
    public static final AtomicIntegerFieldUpdater<Node> statusUpdater = AtomicIntegerFieldUpdater.newUpdater(Node.class, "status");

    public static final int UNMARKED = 0;
    public static final int MARKED = 1;
    public static final int FINALIZED = 2;

    public static class Descriptor {
        Node A, B, C;
        public Descriptor(Node A, Node B, Node C) {
            this.A = A;
            this.B = B;
            this.C = C;
        }
    }
    private static Descriptor frozen = new Descriptor(null, null, null);

    public static class Node {
        public static final AtomicLongFieldUpdater<Node> tsUpdater = AtomicLongFieldUpdater.newUpdater(Node.class, "ts");
        public static final Node dummyNextv = new Node(0);

        public volatile Node left, right;
        public volatile int status;
        public volatile long ts;
        public long counter;
        public volatile Descriptor leftDesc, rightDesc;

        public Node(long ts) {
            right = null;
            left = dummyNextv; // allows multiple processes to tryAppend with the same B and C
            status = UNMARKED;
            this.ts = ts;
            leftDesc = rightDesc = null;
//            counter = 2;
//            priority = calculatePriority(counter);
        }
    }

//    public static Node getHead() { return Head; }

    public static Node find(Node start, long ts) {
        Node cur = start;
        while(cur != null && cur.ts > ts) cur = cur.left;
        return cur;
    }

    public static Node getOlderVersion(Node node) {
        return node.left;
    }

    private static int lowestSetBit(long c) {
        for(int i = 0; i < 64; i++) {
            if ((c & (1l << i)) != 0)
                return i;
        }
        assert false;
        return 0;
    }

    private static int priority(long c) {
        int k = 0;
        long twok = 1; // 2^k
        while(2*twok <= c) {
            k++;
            twok *= 2;
        }
        if(c == twok) return k;
        else return 2*k + 1 - lowestSetBit(c);
    }

    public static boolean tryAppend(AtomicReferenceFieldUpdater updater, Node node, Node B, Node C) {
//        for(int i = 0; i < 10; i++)
//            System.out.print(calculatePriority(2+i) + " ");
//        System.out.println();
        if(B != null) {
            C.counter = B.counter+1;
            Node A = B.left;
            if(A != null) updateRight.compareAndSet(A, null, B);
        } else C.counter = 2;
        updateLeft.compareAndSet(C, Node.dummyNextv, B);
        if(updater.compareAndSet(node, B, C)) {
            if(B != null) updateRight.compareAndSet(B, null, C);
            return true;
        } else return false;
    }

    public static boolean tryAppend(AtomicReferenceArray array, int idx, Node B, Node C) {
//        for(int i = 0; i < 10; i++)
//            System.out.print(calculatePriority(2+i) + " ");
//        System.out.println();
        if(B != null) {
            C.counter = B.counter+1;
            Node A = B.left;
            if(A != null) updateRight.compareAndSet(A, null, B);
        } else C.counter = 2;
        updateLeft.compareAndSet(C, Node.dummyNextv, B);
        if(array.compareAndSet(idx, B, C)) {
            if(B != null) updateRight.compareAndSet(B, null, C);
            return true;
        } else return false;
    }

    public static void removeSeq(Node B) {
        B.status = MARKED;

    }

    public static void remove(Node B) {
        B.status = MARKED;
        Descriptor desc = B.leftDesc;
        while(desc != frozen) {
            help(desc);
            updateLeftDesc.compareAndSet(B, desc, frozen);
            desc = B.leftDesc;
        }
        desc = B.rightDesc;
        while(desc != frozen) {
            help(desc);
            updateRightDesc.compareAndSet(B, desc, frozen);
            desc = B.rightDesc;
        }
        removeRec(B);
    }

    // TODO: when should heads of version lists be removed?
    private static void removeRec(Node B) {
        Node A = B.left;
        Node C = B.right;
        if(B.status == FINALIZED) return;
        int a, b, c;
        if(A != null) {
            a = priority(A.counter);
        }
        else a = 0;
        if(C != null) {
            c = priority(C.counter);
        }
        else c = 0;
        b = priority(B.counter);
        if(a < b && b > c) {
            if(splice(A, B, C)) {
                if(validAndMarked(A)) {
                    if(validAndMarked(C) && c > a) removeRec(C);
                    else removeRec(A);
                } else if(validAndMarked(C)) {
                    if(validAndMarked(A) && a > c) removeRec(A);
                    else removeRec(C);
                }
            }
        } else if(a > b && b > c) {
            if(spliceUnmarkedLeft(A, B, C) && validAndMarked(C))
                removeRec(C);
        } else if(a < b && b < c) {
            if(spliceUnmarkedRight(A, B, C) && validAndMarked(A))
                removeRec(A);
        }
    }

    private static boolean validAndMarked(Node D) {
        return D != null && D.rightDesc == frozen;
    }

    private static void help(Descriptor desc) {
        if(desc != null && desc != frozen)
            splice(desc.A, desc.B, desc.C);
    }

    private static boolean splice(Node A, Node B, Node C) {
        if(A != null && A.right != B) return false;
        boolean result = statusUpdater.compareAndSet(B, MARKED, FINALIZED);
        if(C != null) updateLeft.compareAndSet(C, B, A);
        if(A != null) updateRight.compareAndSet(A, B, C);
        return result;
    }

    private static boolean spliceUnmarkedLeft(Node A, Node B, Node C) {
        Descriptor oldDesc = A.rightDesc;
        if(A.status != UNMARKED) return false;
        help(oldDesc);
        if(A.right != B) return false;
        Descriptor newDesc = new Descriptor(A, B, C);
        if(updateRightDesc.compareAndSet(A, oldDesc, newDesc)) {
            help(newDesc);
            updateRightDesc.compareAndSet(A, newDesc, null);
            return true;
        } else return false;
    }

    private static boolean spliceUnmarkedRight(Node A, Node B, Node C) {
        Descriptor oldDesc = C.leftDesc;
        if(C.status != UNMARKED) return false;
        help(oldDesc);
        if(C.left != B || (A != null && A.right != B)) return false;
        Descriptor newDesc = new Descriptor(A, B, C);
        if(updateLeftDesc.compareAndSet(C, oldDesc, newDesc)) {
            help(newDesc);
            updateLeftDesc.compareAndSet(C, newDesc, null); // TODO: this means that some nodes might not be frozen properly
            return true;
        } else return false;
    }
}
