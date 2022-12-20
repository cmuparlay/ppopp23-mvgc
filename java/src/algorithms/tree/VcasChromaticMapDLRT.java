package algorithms.tree;

import algorithms.common.Camera;
import algorithms.common.CameraBBF;
import main.support.NodeStats;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

public class VcasChromaticMapDLRT<K extends Comparable<? super K>,V> {
    private final int BATCHING_DEGREE = 1;
    private final int d; // this is the number of violations to allow on a search path before we fix everything on it. if d is zero, then each update fixes any violation it created before returning.
    private static final int DEFAULT_d = 6; // experimentally determined to yield good performance for both random workloads, and operations on sorted sequences
    private final InternalNode root;
    private static final Operation dummy = new Operation();
    private static final AtomicReferenceFieldUpdater<InternalNode, Operation> updateOp = AtomicReferenceFieldUpdater.newUpdater(InternalNode.class, Operation.class, "op");

    // public static final AtomicIntegerArray Announce = new AtomicIntegerArray(MAX_THREADS*PADDING);
    // public static final ArrayList<Node>[][] retiredNodes = new ArrayList[3][MAX_THREADS];

    private boolean isBaseline = false;
    public void setBaseline(boolean baseline) {
        isBaseline = baseline;
    }
    public boolean getBaseline() {
        return isBaseline;
    }

    public VcasChromaticMapDLRT() {
        this(DEFAULT_d);
    }

    public VcasChromaticMapDLRT(final int allowedViolationsPerPath) {
        d = allowedViolationsPerPath;
        root = new InternalNode(null, 1, new InternalNode(null, 1, new LeafNode(null, null, 1), null), null);
    }

    /**
     * Only call if there are no ongoing operations. Clears the RangeTracker data structure and calls remove() on
     * all the nodes returned
     */
    public static void cleanup() {
        for(Node node : (List<Node>) CameraBBF.rt.ejectAll()) {
            node.remove();
        }
    }

    /**
     * size() is NOT a constant time method, and the result is only guaranteed to
     * be consistent if no concurrent updates occur.
     * Note: linearizable size() and iterators can be implemented, so contact
     *       the author if they are needed for some application.
     */
    public final int size() {
        return sequentialSize(root);
    }
    private int sequentialSize(final Node node) {
        if (node == null) return 0;
        if (node instanceof LeafNode) return ((LeafNode)node).getSize();
        InternalNode n = (InternalNode) node;
        return sequentialSize(n.getLeft()) + sequentialSize(n.getRight());
    }

    public final  Integer doublyLinkedVsSinglyLinked() {
        HashSet<Node> visitedSinglyLinked = new HashSet<Node>();
        countNodes(root.left, true, false, visitedSinglyLinked);
        HashSet<Node> visitedDoublyLinked = new HashSet<Node>();
        countNodes(root.left, true, true, visitedDoublyLinked);
//        for(Node n : visitedDoublyLinked)
//            if(!visitedSinglyLinked.contains(n)) {
//                System.out.println(n);
//            }
        return visitedDoublyLinked.size() - visitedSinglyLinked.size();
    }
    
    public final NodeStats countNodes(final boolean countVersionList) {
        HashSet<Node> visited = new HashSet<Node>();
        return countNodes(root.left, countVersionList, false, visited);
    }

    public final NodeStats countNodes(final Node node, final boolean countVersionList, final boolean useBackPointers, HashSet<Node> visited) {
        if(node == null) return new NodeStats(0, 0);
        if(visited.contains(node)) return new NodeStats(0, 0);
        visited.add(node);
        NodeStats stats = new NodeStats(0, 0);
        if(countVersionList) {
            Node olderVersion = node.leftv;
            if (olderVersion != null) {
//                assert olderVersion.right == node;
                if (!(olderVersion instanceof Node)) {
                    System.out.println("Other nodes got mixed into the version list");
                }
                stats.add(countNodes((Node) olderVersion, countVersionList, useBackPointers, visited));
            }
            if(useBackPointers) {
                Node newerVersion = node.rightv;
//                assert newerVersion == null || newerVersion.left == node;
                stats.add(countNodes((Node) newerVersion, countVersionList, useBackPointers, visited));
            }
        }

        if(node instanceof LeafNode)
            stats.externalNodes++;
        else {
            stats.internalNodes++;
            stats.add(countNodes(((InternalNode)node).getLeft(), countVersionList, useBackPointers, visited));
            stats.add(countNodes(((InternalNode)node).getRight(), countVersionList, useBackPointers, visited));
        }
        return stats;
    }

    public final double avgVersionListLengths() {
        double sum = 0;
        ArrayList<Integer> lengths = versionListLengths();
        for(Integer i : lengths) sum += i;
        return sum / lengths.size();
    }

    public final int maxVersionListLengths() {
        int mx = 0;
        ArrayList<Integer> lengths = versionListLengths();
        for(Integer i : lengths) mx = Math.max(i, mx);
        return mx;
    }

    public final ArrayList<Integer> versionListLengths() {
        HashSet<Node> visited = new HashSet<Node>();
        ArrayList<Integer> lengths = new ArrayList<Integer>();
        versionListLengths(root.left, visited, lengths);
        return lengths;
    }

    public final void versionListLengths(final Node node, HashSet<Node> visited, ArrayList<Integer> lengths) {
        if(node == null) return;
        if(visited.contains(node)) return;
        visited.add(node);
        versionListLengths(node.leftv, visited, lengths);
        if(node instanceof InternalNode) {
            InternalNode inode = (InternalNode) node;
            if(inode.left != null) lengths.add(listLength(inode.left));
            if(inode.right != null) lengths.add(listLength(inode.right));
            versionListLengths(inode.left, visited, lengths);
            versionListLengths(inode.right, visited, lengths);
        }
    }

    public final int totalLengthOfReachableVersionLists() {
        return totalLengthOfReachableVersionLists(root.left);
    }

    public final int totalLengthOfReachableVersionLists(final Node node) {
        if(node != null && node instanceof InternalNode) {
            InternalNode inode = (InternalNode) node;
            return listLength(inode.left) + listLength(inode.right) +
                    totalLengthOfReachableVersionLists(inode.left) + totalLengthOfReachableVersionLists(inode.right);
        }
        return 0;
    }

    // reachable nodes without traversing nextv pointers
    public final HashSet<Node> reachableNodes() {
        HashSet<Node> reachable = new HashSet<Node>();
        reachableNodes(root.left, reachable);
        return reachable;
    }

    public final void reachableNodes(final Node node, HashSet<Node> reachable) {
        if(node == null) return;
        reachable.add(node);
        if(node instanceof InternalNode) {
            InternalNode inode = (InternalNode) node;
            reachableNodes(inode.left, reachable);
            reachableNodes(inode.right, reachable);
        }
    }

    public final int nodesInReachableVersionLists() {
        HashSet<Node> reachable = reachableNodes();
        HashSet<Node> visited = (HashSet<Node>) reachable.clone();
        int numNodes = 0;
        for(Node node : reachable)
            numNodes += 1 + nodesInVersionList(node.leftv, visited);
        return numNodes; }

    public final int listLength(final Node node) {
        if(node == null) return 0;
        return 1 + listLength(node.leftv);
    }

    public final int nodesInVersionList(final Node node, HashSet<Node> visited) {
        if(node == null) return 0;
        return nodesInSubtree(node, visited) + nodesInVersionList(node.leftv, visited);
    }

    // returns # of nodes in subtree, ignoring nodes in version lists
    public final int nodesInSubtree(final Node node, HashSet<Node> visited) {
        if(node == null || visited.contains(node)) return 0;
        visited.add(node);
        if(node instanceof LeafNode) return 1;
        InternalNode inode = (InternalNode) node;
        return 1 + nodesInSubtree(inode.left, visited) + nodesInSubtree(inode.right, visited);
    }

    public final boolean containsKey(final K key) {
        return get(key) != null;
    }

    public final V get(final K key) {
        InternalNode p = (InternalNode) root.getLeft();
        while(true) {
            Node l = (p.key == null || key.compareTo((K) p.key) < 0) ? p.getLeft() : p.getRight();
            if(l instanceof LeafNode) return (V) ((LeafNode)l).getValue(key);
            p = (InternalNode) l;
        }
    }

    private final V get(final K key, final long ts) {
        InternalNode p = (InternalNode) root.getLeft(ts);
        while(true) {
            Node l = (p.key == null || key.compareTo((K) p.key) < 0) ? p.getLeft(ts) : p.getRight(ts);
            // if(l == null) {
            //     System.out.println(key + ", " + ts + ", " + p.key + ", " + p.ts + ", " + p.left + ", " + p.left.ts);
            // }
            if(l instanceof LeafNode) return (V) ((LeafNode)l).getValue(key);
            p = (InternalNode) l;
        }   
    }

    // public final V put(final K key, final V value) {
    //     return doPut(key, value, false);
    // }

    public final V putIfAbsent(final K key, final V value) {
        Operation op = null;
        InternalNode p = null;
        LeafNode l = null;
        Node n;
        int count = 0;
        boolean structuralChange = false;
        
        while (true) {
            while (op == null) {
                p = root;
                n = root.getLeft();
                if (n instanceof InternalNode) {
                    count = 0;
                    p = (InternalNode) n;
                    n = p.getLeft(); // note: before executing this line, l must have key infinity, and l.getLeft() must not.
                    while (n instanceof InternalNode) {
                        InternalNode nn = (InternalNode) n;
                        if (d > 0 && (nn.weight > 1 || nn.weight == 0 && p.weight == 0)) ++count;
                        p = nn;
                        n = (key.compareTo((K) nn.key) < 0) ? nn.getLeft() : nn.getRight();
                    }
                }
                l = (LeafNode) n;
                V ret = (V) l.getValue(key);
                if(ret != null) return ret; // if we find the key in the tree already
                if(l.getSize() == BATCHING_DEGREE) {// leaf is full
                    op = createInsertOp(p, l, key, value);
                    structuralChange = true;
                }
                else {
                    op = createInsertReplaceOp(p, l, key, value);
                    structuralChange = false;
                }
            }
            if (helpSCX(op, 0)) {
                // clean up violations if necessary
                if(!structuralChange) return null;
                if (d == 0) {
                    if (p.weight == 0 && l.weight == 1) fixToKey(key);
                } else {
                    if (count >= d) fixToKey(key);
                }
                // we may have found the key and replaced its value (and, if so, the old value is stored in the old node)
                return null;
            }
            op = null;
        }
    }

    public final V remove(final K key) {
        InternalNode gp, p = null, nn;
        LeafNode l = null;
        Node n;
        Operation op = null;
        int count = 0;
        V ret = null;
        boolean structuralChange = false;
        
        while (true) {
            while (op == null) {
                gp = root;
                p = root;
                n = root.getLeft();
                if (n instanceof InternalNode) {
                    nn = (InternalNode) n;
                    count = 0;
                    gp = p;
                    p = nn;
                    n = nn.getLeft(); // note: before executing this line, l must have key infinity, and l.getLeft() must not.
                    while (n instanceof InternalNode) {
                        nn = (InternalNode) n;
                        if (d > 0 && (nn.weight > 1 || nn.weight == 0 && p.weight == 0)) ++count;
                        gp = p;
                        p = nn;
                        n = (key.compareTo((K) nn.key) < 0) ? nn.getLeft() : nn.getRight();
                    }
                }
                l = (LeafNode) n;
                ret = (V) l.getValue(key);
                // the key was not in the tree at the linearization point, so no value was removed
                if(ret == null) return null;
                if(l.getSize() == 1 && !isSentinel(l)) {
                    op = createDeleteOp(gp, p, l);
                    structuralChange = true;
                }
                else {
                    op = createDeleteReplaceOp(p, l, key);
                    structuralChange = false;
                }
            }
            if (helpSCX(op, 0)) {
                // clean up violations if necessary
                if(!structuralChange) return ret;
                if (d == 0) {
                    if (p.weight > 0 && l.weight > 0 && !isSentinel(p)) fixToKey(key);
                } else {
                    if (count >= d) fixToKey(key);
                }
                // we deleted a key, so we return the removed value (saved in the old node)
                return ret;
            }
            op = null;
        }
    }

    public final void fixToKey(final K key) {
        while (true) {
            InternalNode ggp, gp, p, nn;
            Node n = root.getLeft();

            if (n instanceof LeafNode) return; // only sentinels in tree...
            nn = (InternalNode) n;
            ggp = gp = root;
            p = nn;
            n = nn.getLeft(); // note: before executing this line, l must have key infinity, and l.getLeft() must not.
            while (n instanceof InternalNode && n.weight <= 1 && (n.weight != 0 || p.weight != 0)) {
                nn = (InternalNode) n;
                ggp = gp;
                gp = p;
                p = nn;
                n = (key.compareTo((K) nn.key) < 0) ? nn.getLeft() : nn.getRight();
            }

            if (n.weight == 1) return; // if no violation, then the search hit a leaf, so we can stop

            final Operation op = createBalancingOp(ggp, gp, p, n);
            if (op != null) {
                helpSCX(op, 0);
            }
        }
    }

    private boolean isSentinel(final Node node) {
        return (node == ((InternalNode)((InternalNode)root).getLeft()).getLeft() || (node instanceof InternalNode && ((InternalNode)node).key == null));
    }
    
    // This weaker form of LLX does not return a linearizable snapshot.
    // However, we do not use the fact that LLX returns a snapshot anywhere in
    //   the proof of SCX (help), and we do not need the snapshot capability
    //   to satisfy the precondition of SCX (that there be an LLX linked to SCX
    //   for each node in V).
    // Note: using a full LLX slows things by ~3%.
    private Operation weakLLX(final Node r) {
        if(r instanceof InternalNode) {
            InternalNode n = (InternalNode) r;
            final Operation rinfo = n.op;
            final int state = rinfo.state;
            if (state == Operation.STATE_ABORTED || (state == Operation.STATE_COMMITTED && !n.marked)) {
                return rinfo;
            }
            if (rinfo.state == Operation.STATE_INPROGRESS) {
                helpSCX(rinfo, 1);
            } else if (n.op.state == Operation.STATE_INPROGRESS) {
                helpSCX(n.op, 1);
            }
            return null;
        } else {
            return dummy;
        }
    }
    // helper function to use the results of a weakLLX more conveniently
    private boolean weakLLX(final Node r, final int i, final Operation[] ops, final Node[] nodes) {
        if ((ops[i] = weakLLX(r)) == null) return false;
        nodes[i] = r;
        return true;
    }
    
    // this function is essentially an SCX without the creation of V, R, fld, new
    // (which are stored in an operation object).
    // the creation of the operation object is simply inlined in other methods.
    private boolean helpSCX(final Operation op, int i) {
        // get local references to some fields of op, in case we later null out fields of op (to help the garbage collector)
        final Node[] nodes = op.nodes;
        final Operation[] ops = op.ops;
        final Node subtree = op.subtree;
        InternalNode node;
        // if we see aborted or committed, no point in helping (already done).
        // further, if committed, variables may have been nulled out to help the garbage collector.
        // so, we return.
        if (op.state != Operation.STATE_INPROGRESS) return true;
        
        // freeze sub-tree
        for (; i<ops.length; ++i) {
            if(nodes[i] instanceof InternalNode) {
                node = (InternalNode) nodes[i];
                if (!updateOp.compareAndSet(node, ops[i], op) && node.op != op) { // if work was not done
                    if (op.allFrozen) {
                        return true;
                    } else {
                        op.state = Operation.STATE_ABORTED;
                        // help the garbage collector (must be AFTER we set state committed or aborted)
                        op.nodes = null;
                        op.ops = null;
                        op.subtree = null;
                        return false;
                    }
                }
            }
        }
        op.allFrozen = true;
        for (i=1; i<ops.length; ++i) 
            if(nodes[i] instanceof InternalNode)
                ((InternalNode)nodes[i]).marked = true; // finalize all but first node
        
        // CAS in the new sub-tree (child-cas)
        node = (InternalNode) nodes[0];
        if (node.getLeft() == nodes[1]) {
            node.compareAndSetLeft(nodes[1], subtree);
//            assert this.doublyLinkedVsSinglyLinked() == 0;
        } else { // assert: nodes[0].getRight() == nodes[1]
            node.compareAndSetRight(nodes[1], subtree); // splice in new sub-tree (as a right child)
//            assert this.doublyLinkedVsSinglyLinked() == 0;
        }
        op.state = Operation.STATE_COMMITTED;
        
        // help the garbage collector (must be AFTER we set state committed or aborted)
        op.nodes = null;
        op.ops = null;
        op.subtree = null;
        return true;
    }

    private Operation createInsertOp(final InternalNode p, final LeafNode l, final K key, final V value) {
        final Operation[] ops = new Operation[]{null};
        final Node[] nodes = new Node[]{null, l};

        if (!weakLLX(p, 0, ops, nodes)) return null;

        if (l != p.getLeft() && l != p.getRight()) return null;

        // Compute the weight for the new parent node
        final int newWeight = (isSentinel(l) ? 1 : l.weight - 1);               // (maintain sentinel weights at 1)

        // Build new sub-tree
        final LeafNode newLeft, newRight;
        final InternalNode newInternal;
        if(l.shouldBePutLeft(key)) {
            newLeft = l.splitLeftAndPut(key, value);
            newRight = l.splitRight();
        } else {
            newLeft = l.splitLeft();
            newRight = l.splitRightAndPut(key, value);                        
        }
        newInternal = new InternalNode(newRight.key, newWeight, newLeft, newRight);
        return new Operation(nodes, ops, newInternal);
    }
    
    // Just like insert, except this replaces any existing value.
    private Operation createInsertReplaceOp(final InternalNode p, final LeafNode l, final K key, final V value) {
        final Operation[] ops = new Operation[]{null};
        final Node[] nodes = new Node[]{null, l};

        if (!weakLLX(p, 0, ops, nodes)) return null;

        if (l != p.getLeft() && l != p.getRight()) return null;

        // Build new sub-tree
        final Node subtree = l.put(key, value);
        return new Operation(nodes, ops, subtree);
    }

    // Just like insert, except this replaces any existing value.
    private Operation createDeleteReplaceOp(final InternalNode p, final LeafNode l, final K key) {
        final Operation[] ops = new Operation[]{null};
        final Node[] nodes = new Node[]{null, l};

        if (!weakLLX(p, 0, ops, nodes)) return null;

        if (l != p.getLeft() && l != p.getRight()) return null;

        // Build new sub-tree
        final Node subtree = l.remove(key);
        return new Operation(nodes, ops, subtree);
    }

    private Operation createDeleteOp(final InternalNode gp, final InternalNode p, final LeafNode l) {
        final Operation[] ops = new Operation[]{null, null, null};
        final Node[] nodes = new Node[]{null, null, null};

        if (!weakLLX(gp, 0, ops, nodes)) return null;
        if (!weakLLX(p, 1, ops, nodes)) return null;
        
        if (p != gp.getLeft() && p != gp.getRight()) return null;
        final boolean left = (l == p.getLeft());
        if (!left && l != p.getRight()) return null;

        // Read fields for the sibling of l into ops[2], nodes[2] = s
        if (!weakLLX(left ? p.getRight() : p.getLeft(), 2, ops, nodes)) return null;
        final Node s = nodes[2];

        // Now, if the op. succeeds, all structure is guaranteed to be just as we verified

        // Compute weight for the new node (to replace to deleted leaf l and parent p)
        final int newWeight = (isSentinel(p) ? 1 : p.weight + s.weight); // weights of parent + sibling of deleted leaf

        // Build new sub-tree
        final Node newP = s.copy(newWeight);
        return new Operation(nodes, ops, newP);
    }

    private Operation createBalancingOp(final InternalNode f, final InternalNode fX, final InternalNode fXX, final Node fXXX) {
        final Operation opf = weakLLX(f);
        if (opf == null || !f.hasChild(fX)) return null;

        final Operation opfX = weakLLX(fX);
        if (opfX == null) return null;
        final Node fXL = fX.getLeft();
        final Node fXR = fX.getRight();
        final boolean fXXleft = (fXX == fXL);
        if (!fXXleft && fXX != fXR) return null;
        
        final Operation opfXX = weakLLX(fXX);
        if (opfXX == null) return null;
        final Node fXXL = fXX.getLeft();
        final Node fXXR = fXX.getRight();
        final boolean fXXXleft = (fXXX == fXXL);
        if (!fXXXleft && fXXX != fXXR) return null;
        
        // Overweight violation
        if (fXXX.weight > 1) {
            if (fXXXleft) {
                final Operation opfXXL = weakLLX(fXXL);
                if (opfXXL == null) return null;
                return createOverweightLeftOp(f, fX, fXX, fXXL, opf, opfX, opfXX, opfXXL, fXL, fXR, fXXR, fXXleft);
            } else {
                final Operation opfXXR = weakLLX(fXXR);
                if (opfXXR == null) return null;
                return createOverweightRightOp(f, fX, fXX, fXXR, opf, opfX, opfXX, opfXXR, fXR, fXL, fXXL, !fXXleft);
            }
        // Red-red violation
        } else {
            if (fXXleft) {
                if (fXR.weight == 0) {
                    final Operation opfXR = weakLLX(fXR);
                    if (opfXR == null) return null;
                    return createBlkOp(new Node[] {f, fX, fXX, fXR}, new Operation[] {opf, opfX, opfXX, opfXR});
                    
                } else if (fXXXleft) {
                    return createRb1Op(new Node[] {f, fX, fXX}, new Operation[] {opf, opfX, opfXX});
                    
                } else {
                    final Operation opfXXR = weakLLX(fXXR);
                    if (opfXXR == null) return null;
                    return createRb2Op(new Node[] {f, fX, fXX, fXXR}, new Operation[] {opf, opfX, opfXX, opfXXR});
                }
            } else {
                if (fXL.weight == 0) {
                    final Operation opfXL = weakLLX(fXL);
                    if (opfXL == null) return null;
                    return createBlkOp(new Node[] {f, fX, fXL, fXX}, new Operation[] {opf, opfX, opfXL, opfXX});
                    
                } else if (!fXXXleft) {
                    return createRb1SymOp(new Node[] {f, fX, fXX}, new Operation[] {opf, opfX, opfXX});
                    
                } else {
                    final Operation opfXXL = weakLLX(fXXL);
                    if (opfXXL == null) return null;
                    return createRb2SymOp(new Node[] {f, fX, fXX, fXXL}, new Operation[] {opf, opfX, opfXX, opfXXL});
                }
            }
        }
    }
    
    private Operation createOverweightLeftOp(final Node f,
                                             final Node fX,
                                             final Node fXX,
                                             final Node fXXL,
                                             final Operation opf,
                                             final Operation opfX,
                                             final Operation opfXX,
                                             final Operation opfXXL,
                                             final Node fXL,
                                             final Node fXR,
                                             final Node fXXR,
                                             final boolean fXXlef) {
        if (fXXR.weight == 0) {
            if (fXX.weight == 0) {
                if (fXXlef) {
                    if (fXR.weight == 0) {
                        final Operation opfXR = weakLLX(fXR);
                        if (opfXR == null) return null;
                        return createBlkOp(new Node[] {f, fX, fXX, fXR}, new Operation[] {opf, opfX, opfXX, opfXR});
                    } else { // assert: fXR.weight > 0
                        final Operation opfXXR = weakLLX(fXXR);
                        if (opfXXR == null) return null;
                        return createRb2Op(new Node[] {f, fX, fXX, fXXR}, new Operation[] {opf, opfX, opfXX, opfXXR});
                    }
                } else { // assert: fXX == fXR
                    if (fXL.weight == 0) {
                        final Operation opfXL = weakLLX(fXL);
                        if (opfXL == null) return null;
                        return createBlkOp(new Node[] {f, fX, fXL, fXX}, new Operation[] {opf, opfX, opfXL, opfXX});
                    } else {
                        return createRb1SymOp(new Node[] {f, fX, fXX}, new Operation[] {opf, opfX, opfXX});
                    }
                }
            } else { // assert: fXX.weight > 0
                final Operation opfXXR = weakLLX(fXXR);
                if (opfXXR == null) return null;
                
                final Node fXXRL = ((InternalNode)fXXR).getLeft();
                final Operation opfXXRL = weakLLX(fXXRL);
                if (opfXXRL == null) return null;
                
                if (fXXRL.weight > 1) {
                    return createW1Op(new Node[] {fX, fXX, fXXL, fXXR, fXXRL}, new Operation[] {opfX, opfXX, opfXXL, opfXXR, opfXXRL});
                } else if (fXXRL.weight == 0) {
                    return createRb2SymOp(new Node[] {fX, fXX, fXXR, fXXRL}, new Operation[] {opfX, opfXX, opfXXR, opfXXRL});
                } else { // assert: fXXRL.weight == 1
                    if(fXXRL instanceof LeafNode) return null;
                    final Node fXXRLR = ((InternalNode)fXXRL).getRight();
                    if(fXXRLR == null) return null;
                    if (fXXRLR.weight == 0) {
                        final Operation opfXXRLR = weakLLX(fXXRLR);
                        if (opfXXRLR == null) return null;
                        return createW4Op(new Node[] {fX, fXX, fXXL, fXXR, fXXRL, fXXRLR}, new Operation[] {opfX, opfXX, opfXXL, opfXXR, opfXXRL, opfXXRLR});
                    } else { // assert: fXXRLR.weight > 0
                        if (fXXRL instanceof LeafNode) return null;
                        final Node fXXRLL = ((InternalNode)fXXRL).getLeft();
                        if (fXXRLL == null) return null;
                        if (fXXRLL.weight == 0) {
                            final Operation opfXXRLL = weakLLX(fXXRLL);
                            if (opfXXRLL == null) return null;
                            return createW3Op(new Node[] {fX, fXX, fXXL, fXXR, fXXRL, fXXRLL}, new Operation[] {opfX, opfXX, opfXXL, opfXXR, opfXXRL, opfXXRLL});
                        } else { // assert: fXXRLL.weight > 0
                            return createW2Op(new Node[] {fX, fXX, fXXL, fXXR, fXXRL}, new Operation[] {opfX, opfXX, opfXXL, opfXXR, opfXXRL});
                        }
                    }
                }
            }
        } else if (fXXR.weight == 1) {
            final Operation opfXXR = weakLLX(fXXR);
            if (opfXXR == null) return null;
            
            if(fXXR instanceof LeafNode) return null;
            final Node fXXRL = ((InternalNode)fXXR).getLeft();
            if (fXXRL == null) return null;
            final Node fXXRR = ((InternalNode)fXXR).getRight(); // note: if fXXRR is null, then fXXRL is null, since tree is always a full binary tree, and children of leaves don't change
            if (fXXRR.weight == 0) {
                final Operation opfXXRR = weakLLX(fXXRR);
                if (opfXXRR == null) return null;
                return createW5Op(new Node[] {fX, fXX, fXXL, fXXR, fXXRR}, new Operation[] {opfX, opfXX, opfXXL, opfXXR, opfXXRR});
            } else if (fXXRL.weight == 0) {
                final Operation opfXXRL = weakLLX(fXXRL);
                if (opfXXRL == null) return null;
                return createW6Op(new Node[] {fX, fXX, fXXL, fXXR, fXXRL}, new Operation[] {opfX, opfXX, opfXXL, opfXXR, opfXXRL});
            } else {
                return createPushOp(new Node[] {fX, fXX, fXXL, fXXR}, new Operation[] {opfX, opfXX, opfXXL, opfXXR});
            }
        } else {
            final Operation opfXXR = weakLLX(fXXR);
            if (opfXXR == null) return null;
            return createW7Op(new Node[] {fX, fXX, fXXL, fXXR}, new Operation[] {opfX, opfXX, opfXXL, opfXXR});
        }
    }
    
    public static abstract class Node {
        public final int weight;
        public static final long TBD = -1;
        
        private static final AtomicReferenceFieldUpdater<Node, Node> updateLeftv = AtomicReferenceFieldUpdater.newUpdater(Node.class, Node.class, "leftv");
        private static final AtomicReferenceFieldUpdater<Node, Node> updateRightv = AtomicReferenceFieldUpdater.newUpdater(Node.class, Node.class, "rightv");
        public static final AtomicLongFieldUpdater<Node> tsUpdater = AtomicLongFieldUpdater.newUpdater(Node.class, "ts");
        public static final Node dummyNextv = new InternalNode(null, 0, null, null);
        // public static final long MARKED = (1l<<63);

        public volatile Node leftv, rightv;
        public volatile long ts;
        public volatile boolean marked; // TODO: combine marked with ts to save space
        
        public Node(final int weight) {
            rightv = null;
            leftv = dummyNextv; // allows multiple processes to tryAppend with the same B and C
            this.ts = TBD;
            marked = false;
            this.weight = weight;
        }

        public void initTS() {
            if (ts == TBD) {
                long curTS = 0;
                if(Camera.INCREMENT_ON_UPDATE) curTS = CameraBBF.takeSnapshot()+1;
                else curTS = CameraBBF.getTimestamp();
                tsUpdater.compareAndSet(this, TBD, curTS);
            }
        }

        public boolean tryAppend(final AtomicReferenceFieldUpdater updater, final Node B, final Node C) {
            if(B != null) {
                Node A = B.leftv;
                if(A != null && A.rightv == null) updateRightv.compareAndSet(A, null, B);
            }
            updateLeftv.compareAndSet(C, Node.dummyNextv, B);
            if(updater.compareAndSet(this, B, C)) {
                if(B != null) updateRightv.compareAndSet(B, null, C);
                return true;
            } else return false;
        }

        public void remove() {
            assert !this.marked;
            this.marked = true;
            // B.marked = true;
            Node leftv = this.leftv;
            Node rightv = this.rightv;
            Node leftvRight = null, rightvLeft = null;

            while(true) {
                while(leftv != null && leftv.marked) leftv = leftv.leftv;
                if(leftv != null) leftvRight = leftv.rightv;
                while(rightv != null && rightv.marked) rightv = rightv.rightv;
                if(rightv != null) rightvLeft = rightv.leftv;
                if((leftv != null && leftv.marked) || (rightv != null && rightv.marked)) continue;
                if(leftv != null && !updateRightv.compareAndSet(leftv, leftvRight, rightv)) continue;
                if(rightv != null && !updateLeftv.compareAndSet(rightv, rightvLeft, leftv)) continue;
                break;
            }
        }
        
        public abstract Node copy(final int weight);
    }

    public static final class LeafNode extends Node {
        public Comparable key;
        public Object value;

        LeafNode(final Comparable key, final Object value, final int weight) {
            super(weight);
            this.key = key;
            this.value = value;
        } 

        public LeafNode copy(final int weight) {
            return new LeafNode(key, value, weight);
        }

        public int getSize() { return key == null ? 0 : 1; }

        long getSum() {
            return key == null ? 0 : ((Integer) key).intValue();
        }

        /**
            Performs a binary search of key in this node's array of keys.
            Precondition: key cannot be null.

            @param key  the key to search for
            @return     value at the index of key if it was found, otherwise null
        */
        private Object getValue(final Comparable key) {
            if(this.key != null && this.key.equals(key)) return value;
            return null;
        }

        /**
            Adds all keys of this node that belong in range [a,b] to ret.

            @param a    the lower limit of the range
            @param b    the upper limit of the range
            @param ret  the stack where keys are saved
        */
        private final void gatherKeys(final Comparable a, final Comparable b, final boolean leftOpen, final boolean rightOpen, final RangeScanResultHolder.Stack ret) {
            if(key != null && a.compareTo(this.key) <= 0 && b.compareTo(this.key) >= 0) ret.push(value);
        }

        /**
            Checks if key should be put in the left half of this node's array of keys.
            Preconditin: getSize() > 0
        */
        private boolean shouldBePutLeft(final Comparable key) {

            return (key.compareTo(this.key) < 0);
        }
        /**
            Copies all keys of this node plus key in newNode.
        */
        private final LeafNode put(final Comparable key, final Object value) {
            return new LeafNode(key, value, weight);
        }

        /**
            Copies all keys of this node except key in newNode.
        */
        private final LeafNode remove(final Comparable key) {
            return new LeafNode(null, null, weight);
        }

        /**
            Copies the left half of this node's array of keys plus key in newNode.
        */
        private final LeafNode splitLeftAndPut(final Comparable key, final Object value) {
            return new LeafNode(key, value, 1);
        }

        /**
            Copies the right half of this node's array of keys plus key in newNode.
        */
        private final LeafNode splitRightAndPut(final Comparable key, final Object value) {
            return new LeafNode(key, value, 1);
        }

        /**
            Copies the left half of this node's array of keys in newNode.
        */
        private final LeafNode splitLeft() {
            return copy(1);
        }

        /**
            Copies the right half of this node's array of keys in newNode.
        */
        private final LeafNode splitRight() {
            return copy(1);
        }
    }

    public static final class InternalNode extends Node {
        public final Comparable key;
        public volatile Node left, right;
        public volatile boolean marked;
        public volatile Operation op;
        
        public static final AtomicReferenceFieldUpdater<InternalNode, Node> updateLeft = AtomicReferenceFieldUpdater.newUpdater(InternalNode.class, Node.class, "left");
        public static final AtomicReferenceFieldUpdater<InternalNode, Node> updateRight = AtomicReferenceFieldUpdater.newUpdater(InternalNode.class, Node.class, "right");

        public InternalNode(final Comparable key, final int weight, final Node left, final Node right) {
            super(weight);
            this.key = key;
            this.left = left;
            this.right = right;
            this.op = dummy;
            if(left != null) {
                if(left.leftv == dummyNextv) {
                    left.leftv = null;
                    // VersionListLinearTime.initNode(left); // guaranteed to succeed
                    left.initTS(); // TODO: can be optimized to avoid reading global timestamp
                }
            }
            if(right != null) {
                if(right.leftv == dummyNextv) {
                    right.leftv = null;
                    // VersionListLinearTime.initNode(right); // guaranteed to succeed
                    right.initTS(); // TODO: can be optimized to avoid reading global timestamp
                }
            }
        }

        Node getLeft() {
            Node head = left;
            if(head == null) return null;
            head.initTS();
            return head;
        }

        Node getRight() {
            Node head = right;
            if(head == null) return null;
            head.initTS();
            return head;
        }

        Node getLeft(long ts) {
            Node node = left;
            if(node == null) return null;
            node.initTS();
            while(node != null && node.ts > ts) node = node.leftv;
            return node;
        }

        Node getRight(long ts) {
            Node node = right;
            if(node == null) return null;
            node.initTS();
            while(node != null && node.ts > ts) node = node.leftv;
            return node;
        }

        // TODO: multiple process can be competing to append a node
        boolean compareAndSetLeft(final Node oldV, Node newV) {
            Node head = left; // head cannot be null
            if(head != null) {
                head.initTS();
            }
            if(head != oldV) return false;
            if(newV == oldV) return true;
//            nextvUpdater.compareAndSet(newV, dummyNextv, oldV);
            // newV.nextv = oldV;
            // newV.ts = TBD;
            
            if(this.tryAppend(updateLeft, head, newV)) {
                newV.initTS();
                if(head != null) {
                    long headts = head.ts;
                    long newts = newV.ts;
                    if(headts == newts) head.remove();
                    else {
                        List<Node> Redundant = CameraBBF.rt.deprecate(head, headts, newts);
                        if(Redundant != null)
                            for(Node node : Redundant)
                                node.remove();
                    }
                }
                return true;
            } else {
                head = left;
                head.initTS();
                return false;
            }
        }

        boolean compareAndSetRight(final Node oldV, Node newV) {
            Node head = right; // head cannot be null
            if(head != null) {
                head.initTS();
            }
            if(head != oldV) return false;
            if(newV == oldV) return true;
//            nextvUpdater.compareAndSet(newV, dummyNextv, oldV);
            // newV.nextv = oldV;
            // newV.ts = TBD;

            if(this.tryAppend(updateRight, head, newV)) {
                newV.initTS();
                if(head != null) {
                    long headts = head.ts;
                    long newts = newV.ts;
                    if(headts == newts) head.remove();
                    else {
                        List<Node> Redundant = CameraBBF.rt.deprecate(head, headts, newts);
                        if(Redundant != null)
                            for(Node node : Redundant)
                                node.remove();
                    }
                }
                return true;
            } else {
                head = right;
                head.initTS();
                return false;
            }
        }

        public final boolean hasChild(final Node node) {
            return node == getLeft() || node == getRight();
        }

        public Node copy(final int weight) {
            return new InternalNode(key, weight, getLeft(), getRight());
        }
    }

    public static final class Operation {
        final static int STATE_INPROGRESS = 0;
        final static int STATE_ABORTED = 1;
        final static int STATE_COMMITTED = 2;

        volatile Node subtree;
        volatile Node[] nodes;
        volatile Operation[] ops;
        volatile int state;
        volatile boolean allFrozen;

        public Operation() {            // create an inactive operation (a no-op) [[ we do this to avoid the overhead of inheritance ]]
            nodes = null; ops = null; subtree = null;
            this.state = STATE_ABORTED;   // cheap trick to piggy-back on a pre-existing check for active operations
        }
        
        public Operation(final Node[] nodes, final Operation[] ops, final Node subtree) {
            this.nodes = nodes;
            this.ops = ops;
            this.subtree = subtree;
        }
    }
    
    // Reference to a thread local variable that is used by
    // RangeScan to return the result of a range query
    private final ThreadLocal<RangeScanResultHolder> rangeScanResult = new ThreadLocal<RangeScanResultHolder>() {
        @Override
        protected RangeScanResultHolder initialValue() {
            return new RangeScanResultHolder();
        }
    };

    /**
        Represents a storage space where the result of a range query operation is saved
        Each thread gets a copy of this variable
    */
    private static final class RangeScanResultHolder {
        private Stack rsResult;

        RangeScanResultHolder() {
            rsResult = new Stack();
        }

        private static final class Stack {
            private final int INIT_SIZE = 16;
            private Object[] stackArray;
            private int head = 0;

            Stack() {
                stackArray = new Object[INIT_SIZE];
            }

            final void clear() {
                head = 0;
            }

            final Object[] getStackArray() {
                return stackArray;
            }

            final int getEffectiveSize() {
                return head;
            }

            final void push(final Object x) {
                if (head == stackArray.length) {
                    final Object[] newStackArray = new Object[stackArray.length*4];
                    System.arraycopy(stackArray, 0, newStackArray, 0, head);
                    stackArray = newStackArray;
                }
                stackArray[head] = x;
                ++head;
            }
        }
    }

    /**
        Executes the tree traversal for rangeScan.
        Precondition: node.versionSeq is not greater than seq

        @param node    the current node of the traversal
        @param ts      the timestamp number of rangeScan operation
        @param a       the lower limit of the range
        @param b       the upper limit of the range
        @param ret     contains the rangeScan result, i.e. all values that correspond to keys
                       held by nodes in the version-seq part of the tree
    */
    private final void scanHelper(final Node node, final long ts, final K a, final K b, final boolean leftOpen, final boolean rightOpen, RangeScanResultHolder.Stack ret) {
        if(node == null) return;
        if (node instanceof LeafNode) {    // node is a leaf
            ((LeafNode)node).gatherKeys(a, b, leftOpen, rightOpen, ret);
        }
        else {
            InternalNode n = (InternalNode) node;
            if(!leftOpen && !rightOpen) {
                scanHelper(n.getLeft(ts), ts, a, b, false, false, ret);
                scanHelper(n.getRight(ts), ts, a, b, false, false, ret);             
            }
            else if (n.key != null && a.compareTo((K) n.key) >= 0)           // node's key is below the lower limit of [a,b]
                scanHelper(n.getRight(ts), ts, a, b, leftOpen, rightOpen, ret);  // traverse its right subtree
            else if (n.key == null || b.compareTo((K) n.key) < 0)       // node's key is above the upper limit of [a,b]
                scanHelper(n.getLeft(ts), ts, a, b, leftOpen, rightOpen, ret);   // traverse its left subtree
            else {
                // node is in [a,b] - traverse both of its subtrees
                scanHelper(n.getLeft(ts), ts, a, b, leftOpen, false, ret);
                scanHelper(n.getRight(ts), ts, a, b, false, rightOpen, ret);
            }
        }
    }

    /**
        Implements the RangeScan operation.
        <p>
        Preconditions:
        <ul>
            <li> a and b cannot be null
            <li> a is less than or equal to b
        <ul>

        @param a  the lower limit of the range
        @param b  the upper limit of the range
        @return   all values of mappings with keys in range [a,b]
    */
    public final Object[] rangeScan(final K a, final K b) {
        long ts = 0;
        if(Camera.INCREMENT_ON_UPDATE) ts = CameraBBF.getTimestamp();
        else ts = CameraBBF.takeSnapshot();
        //System.out.println(ts);
        // Get and initialize rangeScanResultHolder before the start of the tree traversal
        RangeScanResultHolder rangeScanResultHolder = rangeScanResult.get();
        rangeScanResultHolder.rsResult.clear();

        // Start the tree traversal
        scanHelper(root, ts, a, b, true, true, rangeScanResultHolder.rsResult);
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

    /**
     *
     * Code for debugging
     *
     */
     
    private int countNodesOld(final Node node) {
        if (node == null) return 0;
        if (node instanceof LeafNode) return ((LeafNode)node).getSize();
        InternalNode n = (InternalNode) node;
        return 1 + countNodesOld(n.getLeft()) + countNodesOld(n.getRight());
    }

    public final int getNumberOfNodes() {
        return countNodesOld(root);
    }

    private int sumDepths(final Node node, final int depth) {
        if (node == null) return 0;
        if (node instanceof LeafNode) return 1;
        InternalNode n = (InternalNode) node;
        return sumDepths(n.getLeft(), depth+1) + sumDepths(n.getRight(), depth+1);
    }

    public final int getSumOfDepths() {
        return sumDepths(root, 0);
    }
    
    private long getKeysum(final Node node) {
        if (node == null) return 0;
        if (node instanceof LeafNode) return ((LeafNode)node).getSum();
        return getKeysum(((InternalNode)node).getLeft()) + getKeysum(((InternalNode)node).getRight());
    }
    
    // Returns the sum of keys in the tree (the keys in leaves)
    public final long getKeysum() {
        return getKeysum(((InternalNode)((InternalNode)root).getLeft()).getLeft());
    }
    
    /**
     *
     * Computer generated code
     *
     */
    
    private Operation createOverweightRightOp(final Node f,
                                             final Node fX,
                                             final Node fXX,
                                             final Node fXXR,
                                             final Operation opf,
                                             final Operation opfX,
                                             final Operation opfXX,
                                             final Operation opfXXR,
                                             final Node fXR,
                                             final Node fXL,
                                             final Node fXXL,
                                             final boolean fXXright) {
        if (fXXL.weight == 0) {
            if (fXX.weight == 0) {
                if (fXXright) {
                    if (fXL.weight == 0) {
                        final Operation opfXL = weakLLX(fXL);
                        if (opfXL == null) return null;
                        return createBlkOp(new Node[] {f, fX, fXL, fXX},
                                           new Operation[] {opf, opfX, opfXL, opfXX});
                    } else { // assert: fXL.weight > 0
                        final Operation opfXXL = weakLLX(fXXL);
                        if (opfXXL == null) return null;
                        return createRb2SymOp(new Node[] {f, fX, fXX, fXXL},
                                           new Operation[] {opf, opfX, opfXX, opfXXL});
                    }
                } else { // assert: fXX == fXL
                    if (fXR.weight == 0) {
                        final Operation opfXR = weakLLX(fXR);
                        if (opfXR == null) return null;
                        return createBlkOp(new Node[] {f, fX, fXX, fXR},
                                           new Operation[] {opf, opfX, opfXX, opfXR});
                    } else {
                        return createRb1Op(new Node[] {f, fX, fXX},
                                              new Operation[] {opf, opfX, opfXX});
                    }
                }
            } else { // assert: fXX.weight > 0
                final Operation opfXXL = weakLLX(fXXL);
                if (opfXXL == null) return null;
                
                final Node fXXLR = ((InternalNode)fXXL).getRight();
                final Operation opfXXLR = weakLLX(fXXLR);
                if (opfXXLR == null) return null;
                
                if (fXXLR.weight > 1) {
                    return createW1SymOp(new Node[] {fX, fXX, fXXL, fXXR, fXXLR},
                                      new Operation[] {opfX, opfXX, opfXXL, opfXXR, opfXXLR});
                } else if (fXXLR.weight == 0) {
                    return createRb2Op(new Node[] {fX, fXX, fXXL, fXXLR},
                                          new Operation[] {opfX, opfXX, opfXXL, opfXXLR});
                } else { // assert: fXXLR.weight == 1
                    if(fXXLR instanceof LeafNode) return null;
                    final Node fXXLRL = ((InternalNode)fXXLR).getLeft();
                    if (fXXLRL == null) return null;
                    if (fXXLRL.weight == 0) {
                        final Operation opfXXLRL = weakLLX(fXXLRL);
                        if (opfXXLRL == null) return null;
                        return createW4SymOp(new Node[] {fX, fXX, fXXL, fXXR, fXXLR, fXXLRL},
                                          new Operation[] {opfX, opfXX, opfXXL, opfXXR, opfXXLR, opfXXLRL});
                    } else { // assert: fXXLRL.weight > 0
                        if(fXXLR instanceof LeafNode) return null;
                        final Node fXXLRR = ((InternalNode)fXXLR).getRight();
                        if (fXXLRR == null) return null;
                        if (fXXLRR.weight == 0) {
                            final Operation opfXXLRR = weakLLX(fXXLRR);
                            if (opfXXLRR == null) return null;
                            return createW3SymOp(new Node[] {fX, fXX, fXXL, fXXR, fXXLR, fXXLRR},
                                              new Operation[] {opfX, opfXX, opfXXL, opfXXR, opfXXLR, opfXXLRR});
                        } else { // assert: fXXLRR.weight > 0
                            return createW2SymOp(new Node[] {fX, fXX, fXXL, fXXR, fXXLR},
                                              new Operation[] {opfX, opfXX, opfXXL, opfXXR, opfXXLR});
                        }
                    }
                }
            }
        } else if (fXXL.weight == 1) {
            final Operation opfXXL = weakLLX(fXXL);
            if (opfXXL == null) return null;
            
            if (fXXL instanceof LeafNode) return null;
            final Node fXXLR = ((InternalNode)fXXL).getRight();
            if (fXXLR == null) return null;
            final Node fXXLL = ((InternalNode)fXXL).getLeft(); // note: if fXXLL is null, then fXXLR is null, since tree is always a full binary tree, and children of leaves don't change
            if (fXXLL.weight == 0) {
                final Operation opfXXLL = weakLLX(fXXLL);
                if (opfXXLL == null) return null;
                return createW5SymOp(new Node[] {fX, fXX, fXXL, fXXR, fXXLL},
                                  new Operation[] {opfX, opfXX, opfXXL, opfXXR, opfXXLL});
            } else if (fXXLR.weight == 0) {
                final Operation opfXXLR = weakLLX(fXXLR);
                if (opfXXLR == null) return null;
                return createW6SymOp(new Node[] {fX, fXX, fXXL, fXXR, fXXLR},
                                  new Operation[] {opfX, opfXX, opfXXL, opfXXR, opfXXLR});
            } else {
                return createPushSymOp(new Node[] {fX, fXX, fXXL, fXXR},
                                    new Operation[] {opfX, opfXX, opfXXL, opfXXR});
            }
        } else {
            final Operation opfXXL = weakLLX(fXXL);
            if (opfXXL == null) return null;
            return createW7SymOp(new Node[] {fX, fXX, fXXL, fXXR},
                              new Operation[] {opfX, opfXX, opfXXL, opfXXR});
        }
    }
    
    private Operation createBlkOp(final Node[] nodes, final Operation[] ops) {
        final Node nodeXL = nodes[2].copy(1);
        final Node nodeXR = nodes[3].copy(1);
        final int weight = (isSentinel(nodes[1]) ? 1 : nodes[1].weight-1); // root of old subtree is a sentinel
        final Node nodeX = new InternalNode((Comparable) ((InternalNode)nodes[1]).key, weight, nodeXL, nodeXR);
        return new Operation(nodes, ops, nodeX);
    }
    
    private Operation createRb1Op(final Node[] nodes, final Operation[] ops) {
        final Node nodeXR = new InternalNode((Comparable) ((InternalNode)nodes[1]).key, 0, ((InternalNode)nodes[2]).getRight(), ((InternalNode)nodes[1]).getRight());
        final int weight = nodes[1].weight;
        final Node nodeX = new InternalNode((Comparable) ((InternalNode)nodes[2]).key, weight, ((InternalNode)nodes[2]).getLeft(), nodeXR);
        return new Operation(nodes, ops, nodeX);
    }
    
    private Operation createRb2Op(final Node[] nodes, final Operation[] ops) {
        final Node nodeXL = new InternalNode((Comparable) ((InternalNode)nodes[2]).key, 0, ((InternalNode)nodes[2]).getLeft(), ((InternalNode)nodes[3]).getLeft());
        final Node nodeXR = new InternalNode((Comparable) ((InternalNode)nodes[1]).key, 0, ((InternalNode)nodes[3]).getRight(), ((InternalNode)nodes[1]).getRight());
        final int weight = nodes[1].weight;
        final Node nodeX = new InternalNode((Comparable) ((InternalNode)nodes[3]).key, weight, nodeXL, nodeXR);
        return new Operation(nodes, ops, nodeX);
    }
    
    private Operation createPushOp(final Node[] nodes, final Operation[] ops) {
        final Node nodeXXL = nodes[2].copy(nodes[2].weight-1);
        final Node nodeXXR = nodes[3].copy(0);
        final int weight = (isSentinel(nodes[1]) ? 1 : nodes[1].weight+1); // root of old subtree is a sentinel
        final Node nodeXX = new InternalNode((Comparable) ((InternalNode)nodes[1]).key, weight, nodeXXL, nodeXXR);
        return new Operation(nodes, ops, nodeXX);
    }
    
    private Operation createW1Op(final Node[] nodes, final Operation[] ops) {
        final Node nodeXXLL = nodes[2].copy(nodes[2].weight-1);
        final Node nodeXXLR = nodes[4].copy(nodes[4].weight-1);
        final Node nodeXXL = new InternalNode((Comparable) ((InternalNode)nodes[1]).key, 1, nodeXXLL, nodeXXLR);
        final int weight = nodes[1].weight;
        final Node nodeXX = new InternalNode((Comparable) ((InternalNode)nodes[3]).key, weight, nodeXXL, ((InternalNode)nodes[3]).getRight());
        return new Operation(nodes, ops, nodeXX);
    }
    
    private Operation createW2Op(final Node[] nodes, final Operation[] ops) {
        final Node nodeXXLL = nodes[2].copy(nodes[2].weight-1);
        final Node nodeXXLR = nodes[4].copy(0);
        final Node nodeXXL = new InternalNode((Comparable) ((InternalNode)nodes[1]).key, 1, nodeXXLL, nodeXXLR);
        final int weight = nodes[1].weight;
        final Node nodeXX = new InternalNode((Comparable) ((InternalNode)nodes[3]).key, weight, nodeXXL, ((InternalNode)nodes[3]).getRight());
        return new Operation(nodes, ops, nodeXX);
    }
    
    private Operation createW3Op(final Node[] nodes, final Operation[] ops) {
        final Node nodeXXLLL = nodes[2].copy(nodes[2].weight-1);
        final Node nodeXXLL = new InternalNode((Comparable) ((InternalNode)nodes[1]).key, 1, nodeXXLLL, ((InternalNode)nodes[5]).getLeft());
        final Node nodeXXLR = new InternalNode((Comparable) ((InternalNode)nodes[4]).key, 1, ((InternalNode)nodes[5]).getRight(), ((InternalNode)nodes[4]).getRight());
        final Node nodeXXL = new InternalNode((Comparable) ((InternalNode)nodes[5]).key, 0, nodeXXLL, nodeXXLR);
        final int weight = nodes[1].weight;
        final Node nodeXX = new InternalNode((Comparable) ((InternalNode)nodes[3]).key, weight, nodeXXL, ((InternalNode)nodes[3]).getRight());
        return new Operation(nodes, ops, nodeXX);
    }
    
    private Operation createW4Op(final Node[] nodes, final Operation[] ops) {
        final Node nodeXXLL = nodes[2].copy(nodes[2].weight-1);
        final Node nodeXXL = new InternalNode((Comparable) ((InternalNode)nodes[1]).key, 1, nodeXXLL, ((InternalNode)nodes[4]).getLeft());
        final Node nodeXXRL = nodes[5].copy(1);
        final Node nodeXXR = new InternalNode((Comparable) ((InternalNode)nodes[3]).key, 0, nodeXXRL, ((InternalNode)nodes[3]).getRight());
        final int weight = nodes[1].weight;
        final Node nodeXX = new InternalNode((Comparable) ((InternalNode)nodes[4]).key, weight, nodeXXL, nodeXXR);
        return new Operation(nodes, ops, nodeXX);
    }
    
    private Operation createW5Op(final Node[] nodes, final Operation[] ops) {
        final Node nodeXXLL = nodes[2].copy(nodes[2].weight-1);
        final Node nodeXXL = new InternalNode((Comparable) ((InternalNode)nodes[1]).key, 1, nodeXXLL, ((InternalNode)nodes[3]).getLeft());
        final Node nodeXXR = nodes[4].copy(1);
        final int weight = nodes[1].weight;
        final Node nodeXX = new InternalNode((Comparable) ((InternalNode)nodes[3]).key, weight, nodeXXL, nodeXXR);
        return new Operation(nodes, ops, nodeXX);
    }
    
    private Operation createW6Op(final Node[] nodes, final Operation[] ops) {
        final Node nodeXXLL = nodes[2].copy(nodes[2].weight-1);
        final Node nodeXXL = new InternalNode((Comparable) ((InternalNode)nodes[1]).key, 1, nodeXXLL, ((InternalNode)nodes[4]).getLeft());
        final Node nodeXXR = new InternalNode((Comparable) ((InternalNode)nodes[3]).key, 1, ((InternalNode)nodes[4]).getRight(), ((InternalNode)nodes[3]).getRight());
        final int weight = nodes[1].weight;
        final Node nodeXX = new InternalNode((Comparable) ((InternalNode)nodes[4]).key, weight, nodeXXL, nodeXXR);
        return new Operation(nodes, ops, nodeXX);
    }
    
    private Operation createW7Op(final Node[] nodes, final Operation[] ops) {
        final Node nodeXXL = nodes[2].copy(nodes[2].weight-1);
        final Node nodeXXR = nodes[3].copy(nodes[3].weight-1);
        final int weight = (isSentinel(nodes[1]) ? 1 : nodes[1].weight+1); // root of old subtree is a sentinel
        final Node nodeXX = new InternalNode((Comparable) ((InternalNode)nodes[1]).key, weight, nodeXXL, nodeXXR);
        return new Operation(nodes, ops, nodeXX);
    }
    
    private Operation createRb1SymOp(final Node[] nodes, final Operation[] ops) {
        final Node nodeXL = new InternalNode((Comparable) ((InternalNode)nodes[1]).key, 0, ((InternalNode)nodes[1]).getLeft(), ((InternalNode)nodes[2]).getLeft());
        final int weight = nodes[1].weight;
        final Node nodeX = new InternalNode((Comparable) ((InternalNode)nodes[2]).key, weight, nodeXL, ((InternalNode)nodes[2]).getRight());
        return new Operation(nodes, ops, nodeX);
    }
    
    private Operation createRb2SymOp(final Node[] nodes, final Operation[] ops) {
        final Node nodeXL = new InternalNode((Comparable) ((InternalNode)nodes[1]).key, 0, ((InternalNode)nodes[1]).getLeft(), ((InternalNode)nodes[3]).getLeft());
        final Node nodeXR = new InternalNode((Comparable) ((InternalNode)nodes[2]).key, 0, ((InternalNode)nodes[3]).getRight(), ((InternalNode)nodes[2]).getRight());
        final int weight = nodes[1].weight;
        final Node nodeX = new InternalNode((Comparable) ((InternalNode)nodes[3]).key, weight, nodeXL, nodeXR);
        return new Operation(nodes, ops, nodeX);
    }
    
    private Operation createPushSymOp(final Node[] nodes, final Operation[] ops) {
        final Node nodeXXL = nodes[2].copy(0);
        final Node nodeXXR = nodes[3].copy(nodes[3].weight-1);
        final int weight = (isSentinel(nodes[1]) ? 1 : nodes[1].weight+1); // root of old subtree is a sentinel
        final Node nodeXX = new InternalNode((Comparable) ((InternalNode)nodes[1]).key, weight, nodeXXL, nodeXXR);
        return new Operation(nodes, ops, nodeXX);
    }
    
    private Operation createW1SymOp(final Node[] nodes, final Operation[] ops) {
        final Node nodeXXRL = nodes[4].copy(nodes[4].weight-1);
        final Node nodeXXRR = nodes[3].copy(nodes[3].weight-1);
        final Node nodeXXR = new InternalNode((Comparable) ((InternalNode)nodes[1]).key, 1, nodeXXRL, nodeXXRR);
        final int weight = nodes[1].weight;
        final Node nodeXX = new InternalNode((Comparable) ((InternalNode)nodes[2]).key, weight, ((InternalNode)nodes[2]).getLeft(), nodeXXR);
        return new Operation(nodes, ops, nodeXX);
    }
    
    private Operation createW2SymOp(final Node[] nodes, final Operation[] ops) {
        final Node nodeXXRL = nodes[4].copy(0);
        final Node nodeXXRR = nodes[3].copy(nodes[3].weight-1);
        final Node nodeXXR = new InternalNode((Comparable) ((InternalNode)nodes[1]).key, 1, nodeXXRL, nodeXXRR);
        final int weight = nodes[1].weight;
        final Node nodeXX = new InternalNode((Comparable) ((InternalNode)nodes[2]).key, weight, ((InternalNode)nodes[2]).getLeft(), nodeXXR);
        return new Operation(nodes, ops, nodeXX);
    }
    
    private Operation createW3SymOp(final Node[] nodes, final Operation[] ops) {
        final Node nodeXXRL = new InternalNode((Comparable) ((InternalNode)nodes[4]).key, 1, ((InternalNode)nodes[4]).getLeft(), ((InternalNode)nodes[5]).getLeft());
        final Node nodeXXRRR = nodes[3].copy(nodes[3].weight-1);
        final Node nodeXXRR = new InternalNode((Comparable) ((InternalNode)nodes[1]).key, 1, ((InternalNode)nodes[5]).getRight(), nodeXXRRR);
        final Node nodeXXR = new InternalNode((Comparable) ((InternalNode)nodes[5]).key, 0, nodeXXRL, nodeXXRR);
        final int weight = nodes[1].weight;
        final Node nodeXX = new InternalNode((Comparable) ((InternalNode)nodes[2]).key, weight, ((InternalNode)nodes[2]).getLeft(), nodeXXR);
        return new Operation(nodes, ops, nodeXX);
    }
    
    private Operation createW4SymOp(final Node[] nodes, final Operation[] ops) {
        final Node nodeXXLR = nodes[5].copy(1);
        final Node nodeXXL = new InternalNode((Comparable) ((InternalNode)nodes[2]).key, 0, ((InternalNode)nodes[2]).getLeft(), nodeXXLR);
        final Node nodeXXRR = nodes[3].copy(nodes[3].weight-1);
        final Node nodeXXR = new InternalNode((Comparable) ((InternalNode)nodes[1]).key, 1, ((InternalNode)nodes[4]).getRight(), nodeXXRR);
        final int weight = nodes[1].weight;
        final Node nodeXX = new InternalNode((Comparable) ((InternalNode)nodes[4]).key, weight, nodeXXL, nodeXXR);
        return new Operation(nodes, ops, nodeXX);
    }
    
    private Operation createW5SymOp(final Node[] nodes, final Operation[] ops) {
        final Node nodeXXL = nodes[4].copy(1);
        final Node nodeXXRR = nodes[3].copy(nodes[3].weight-1);
        final Node nodeXXR = new InternalNode((Comparable) ((InternalNode)nodes[1]).key, 1, ((InternalNode)nodes[2]).getRight(), nodeXXRR);
        final int weight = nodes[1].weight;
        final Node nodeXX = new InternalNode((Comparable) ((InternalNode)nodes[2]).key, weight, nodeXXL, nodeXXR);
        return new Operation(nodes, ops, nodeXX);
    }
    
    private Operation createW6SymOp(final Node[] nodes, final Operation[] ops) {
        final Node nodeXXL = new InternalNode((Comparable) ((InternalNode)nodes[2]).key, 1, ((InternalNode)nodes[2]).getLeft(), ((InternalNode)nodes[4]).getLeft());
        final Node nodeXXRR = nodes[3].copy(nodes[3].weight-1);
        final Node nodeXXR = new InternalNode((Comparable) ((InternalNode)nodes[1]).key, 1, ((InternalNode)nodes[4]).getRight(), nodeXXRR);
        final int weight = nodes[1].weight;
        final Node nodeXX = new InternalNode((Comparable) ((InternalNode)nodes[4]).key, weight, nodeXXL, nodeXXR);
        return new Operation(nodes, ops, nodeXX);
    }

    private Operation createW7SymOp(final Node[] nodes, final Operation[] ops) {
        final Node nodeXXL = nodes[2].copy(nodes[2].weight-1);
        final Node nodeXXR = nodes[3].copy(nodes[3].weight-1);
        final int weight = (isSentinel(nodes[1]) ? 1 : nodes[1].weight+1); // root of old subtree is a sentinel
        final Node nodeXX = new InternalNode((Comparable) ((InternalNode)nodes[1]).key, weight, nodeXXL, nodeXXR);
        return new Operation(nodes, ops, nodeXX);
    }

}
