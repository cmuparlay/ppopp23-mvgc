package adapters;

import main.support.NodeStats;

import java.util.ArrayList;

public interface ProfileableV2 extends Profileable{

    double avgNodesTraversedDuringReadVersion();

    void resetStats();
}
