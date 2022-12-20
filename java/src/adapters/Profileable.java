package adapters;

import main.support.NodeStats;

import java.util.ArrayList;

public interface Profileable {
    NodeStats countNodes(final boolean countVersionList);

    double avgVersionListLengths();

    int maxVersionListLengths();

    ArrayList<Integer> versionListLengths();

    int totalLengthOfReachableVersionLists();

    int nodesInReachableVersionLists();

    /**
     * @return Number of nodes only reachable with back pointers. Returns 0 if version lists singly linked.
     */
    Integer doublyLinkedVsSinglyLinked();

}
