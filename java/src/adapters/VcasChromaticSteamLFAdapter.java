
package adapters;

import main.support.*;

import java.util.ArrayList;

import algorithms.tree.VcasChromaticMapSteamLF;

public class VcasChromaticSteamLFAdapter<K extends Comparable<? super K>> extends AbstractAdapter<K> implements SetInterface<K>, ProfileableV2 {
    public VcasChromaticMapSteamLF<K,K> tree;

    // public VcasBatchChromaticAdapter() {
    //     tree = new VcasBatchChromaticMap();
    // }

    // public VcasBatchChromaticAdapter(final int allowedViolations) {
    //     tree = new VcasBatchChromaticMap(allowedViolations);
    // }

    public VcasChromaticSteamLFAdapter() {
        tree = new VcasChromaticMapSteamLF();
    }

    public VcasChromaticSteamLFAdapter(final int allowedViolations) {
        tree = new VcasChromaticMapSteamLF(allowedViolations);
    }

    public boolean contains(K key) {
        return tree.containsKey(key);
    }
    
    public boolean add(K key, Random rng) {
        return tree.putIfAbsent(key, key) == null;
        //return tree.put(key, key) == null;
    }

    public K get(K key) {
        return tree.get(key);
    }

    public boolean remove(K key, Random rng) {
        return tree.remove(key) != null;
    }

    public void addListener(OperationListener l) {

    }
    
    @Override
    public Object rangeQuery(K lo, K hi, int rangeSize, Random rng) {
        return tree.rangeScan(lo, hi);
    }

    public int size() {
        return sequentialSize();
    }

    public KSTNode<K> getRoot() {
        return null;
    }
    
    public double getAverageDepth() {
        return tree.getSumOfDepths() / (double) tree.getNumberOfNodes();
    }

    public int getSumOfDepths() {
        return tree.getSumOfDepths();
    }

    public int sequentialSize() {
        return tree.size();
    }
    
    public boolean supportsKeysum() {
        return true;
    }
    public long getKeysum() {
        return tree.getKeysum();
    }

    public double getRebalanceProbability() {
        return -1;
    }

    @Override
    public String toString() {
        return tree.toString();
    }
    
    public void disableRotations() {

    }

    public void enableRotations() {

    }

    public final NodeStats countNodes(final boolean countVersionList) { return tree.countNodes(countVersionList); }

    public final double avgVersionListLengths() { return tree.avgVersionListLengths(); }

    public final int maxVersionListLengths() { return tree.maxVersionListLengths(); }

    public final ArrayList<Integer> versionListLengths() { return tree.versionListLengths(); }

    public final int totalLengthOfReachableVersionLists() { return tree.totalLengthOfReachableVersionLists(); }

    public final int nodesInReachableVersionLists() { return tree.nodesInReachableVersionLists(); }

    public final  Integer doublyLinkedVsSinglyLinked() { return 0; }

    public final double avgNodesTraversedDuringReadVersion() {
        return tree.avgNodesTraversedDuringReadVersion();
    }

    public void resetStats() {
        tree.resetStats();
    }
}
