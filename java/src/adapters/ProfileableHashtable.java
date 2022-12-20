package adapters;

import java.util.ArrayList;

public interface ProfileableHashtable {
    double avgNodesPerVersionList();

    double avgVersionListLengths();

    int maxVersionListLengths();

    int totalNodes(boolean traverseVersionLists);

    ArrayList<Integer> versionListLengths();

    ArrayList<Integer> countNodes(boolean traverseVersionLists);

    double avgNodesTraversedDuringRemove();

    double avgNodesTraversedDuringReadVersion();

    void resetStats();
}
