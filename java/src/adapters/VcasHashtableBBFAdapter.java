
package adapters;

import algorithms.hashtable.VcasHashtableBBF;
import main.support.KSTNode;
import main.support.OperationListener;
import main.support.Random;
import main.support.SetInterface;

import java.util.ArrayList;
import java.util.List;

public class VcasHashtableBBFAdapter<K extends Comparable<? super K>> extends AbstractAdapter<K> implements SetInterface<K>, ProfileableHashtable {
    public VcasHashtableBBF<K,K> ds;

    public VcasHashtableBBFAdapter() {
        ds = new VcasHashtableBBF();
    }

    public VcasHashtableBBFAdapter(final int tableSize) {
        ds = new VcasHashtableBBF(tableSize);
    }

    public void cleanup() { VcasHashtableBBF.cleanup(); }

    public boolean contains(K key) {
        return ds.containsKey(key);
    }
    
    public boolean add(K key, Random rng) {
        return ds.putIfAbsent(key, key);
    }

    public K get(K key) {
        return ds.get(key);
    }

    public boolean remove(K key, Random rng) {
        return ds.remove(key);
    }

    public void addListener(OperationListener l) {

    }

    @Override
    public Object rangeQuery(K lo, K hi, int rangeSize, Random rng) {
        return ds.rangeScan(lo, hi);
    }

    // @Override
    // public Object[] multiSearchNonAtomic(K[] keys) {
    //     return tree.multiSearchNonAtomic(keys);
    // }

    public int size() {
        return (int) ds.sequentialSize();
    }

    public KSTNode<K> getRoot() {
        return null;
    }
    
    public double getAverageDepth() {
        return 0;
    }

    public int getSumOfDepths() {
        return 0;
    }

    public int sequentialSize() {
        return size();
    }
    
    public boolean supportsKeysum() {
        return true;
    }
    public long getKeysum() {
        return ds.getKeysum();
    }

    public double getRebalanceProbability() {
        return -1;
    }

    @Override
    public String toString() {
        return ds.toString();
    }
    
    public void disableRotations() {

    }

    public void enableRotations() {

    }
    public double avgNodesPerVersionList() {
        List<Integer> numNodes = countNodes(true);
        long sum = 0;
        for(Integer len : numNodes)
            sum += len;
        return 1.0*sum / numNodes.size();
    }

    public int totalNodes(boolean traverseVersionLists) {
        List<Integer> numNodes = countNodes(traverseVersionLists);
        int sum = 0;
        for(Integer len : numNodes)
            sum += len;
        return sum;
    }

    public double avgVersionListLengths() {
        List<Integer> lengths = versionListLengths();
        long sum = 0;
        for(Integer len : lengths)
            sum += len;
        return 1.0*sum / lengths.size();
    }

    public int maxVersionListLengths() {
        List<Integer> lengths = versionListLengths();
        int mx = 0;
        for(Integer len : lengths)
            mx = Math.max(mx, len);
        return mx;
    }

    public ArrayList<Integer> versionListLengths() {
        return ds.versionListLengths();
    }

    public ArrayList<Integer> countNodes(boolean traverseVersionLists) {
        return ds.countNodes(traverseVersionLists);
    }

    public double avgNodesTraversedDuringRemove() {
        return ds.avgNodesTraversedDuringRemove();
    }

    public double avgNodesTraversedDuringReadVersion() {
        return ds.avgNodesTraversedDuringReadVersion();
    }

    public void resetStats() {
        ds.resetStats();
    }
}
