
package main.support;

import org.deuce.transform.Exclude;

import adapters.*;

import java.util.ArrayList;

public class Factories {
      // central list of factory classes for all supported data structures

    public static final ArrayList<TreeFactory<Integer>> factories =
            new ArrayList<TreeFactory<Integer>>();
    static {
        factories.add(new VcasChromaticEpochFactory<Integer>());
        factories.add(new VcasChromaticSLRTFactory<Integer>());
        factories.add(new VcasChromaticSteamLFFactory<Integer>());
        factories.add(new VcasChromaticBBFFactory<Integer>());
        factories.add(new VcasChromaticDLRTFactory<Integer>());

        factories.add(new VcasHashtableSteamLFFactory<>());
        factories.add(new VcasHashtableDLRTFactory<Integer>());
        factories.add(new VcasHashtableBBFFactory<Integer>());
        factories.add(new VcasHashtableSLRTFactory<Integer>());
        factories.add(new VcasHashtableEpochFactory<Integer>());
    }

    // factory classes for each supported data structure

    @Exclude
    protected static class VcasChromaticEpochFactory<K> extends TreeFactory<K> {
        Object param;
        public SetInterface<K> newTree(final Object param) {
            this.param = param;
            return param.toString().isEmpty() ? new VcasChromaticEpochAdapter()
                                              : new VcasChromaticEpochAdapter(Integer.parseInt(param.toString()));
        }
        public String getName() { return "VcasChromaticBSTEpoch"; }
    }

    @Exclude
    protected static class VcasChromaticSteamLFFactory<K> extends TreeFactory<K> {
        Object param;
        public SetInterface<K> newTree(final Object param) {
            this.param = param;
            return param.toString().isEmpty() ? new VcasChromaticSteamLFAdapter()
                                              : new VcasChromaticSteamLFAdapter(Integer.parseInt(param.toString()));
        }
        public String getName() { return "VcasChromaticBSTSteamLF"; }
    }

    @Exclude
    protected static class VcasChromaticBBFFactory<K> extends TreeFactory<K> {
        Object param;
        public SetInterface<K> newTree(final Object param) {
            this.param = param;
            return param.toString().isEmpty() ? new VcasChromaticBBFAdapter()
                    : new VcasChromaticBBFAdapter(Integer.parseInt(param.toString()));
        }
        public String getName() { return "VcasChromaticBSTBBF"; }
    }

    @Exclude
    protected static class VcasChromaticDLRTFactory<K> extends TreeFactory<K> {
        Object param;
        public SetInterface<K> newTree(final Object param) {
            this.param = param;
            return param.toString().isEmpty() ? new VcasChromaticDLRTAdapter()
                    : new VcasChromaticDLRTAdapter(Integer.parseInt(param.toString()));
        }
        public String getName() { return "VcasChromaticBSTDLRT"; }
    }

    @Exclude
    protected static class VcasChromaticSLRTFactory<K> extends TreeFactory<K> {
        Object param;
        public SetInterface<K> newTree(final Object param) {
            this.param = param;
            return param.toString().isEmpty() ? new VcasChromaticSLRTAdapter()
                    : new VcasChromaticSLRTAdapter(Integer.parseInt(param.toString()));
        }
        public String getName() { return "VcasChromaticBSTSLRT"; }
    }

    @Exclude
    protected static class VcasHashtableEpochFactory<K> extends TreeFactory<K> {
        Object param;
        public SetInterface<K> newTree(final Object param) {
            this.param = param;
            return param.toString().isEmpty() ? new VcasHashtableEpochAdapter()
                    : new VcasHashtableEpochAdapter(Integer.parseInt(param.toString()));
        }
        public String getName() { return "VcasHashtableEpoch"; }
    }

    @Exclude
    protected static class VcasHashtableSLRTFactory<K> extends TreeFactory<K> {
        Object param;
        public SetInterface<K> newTree(final Object param) {
            this.param = param;
            return param.toString().isEmpty() ? new VcasHashtableSLRTAdapter()
                    : new VcasHashtableSLRTAdapter(Integer.parseInt(param.toString()));
        }
        public String getName() { return "VcasHashtableSLRT"; }
    }

    @Exclude
    protected static class VcasHashtableDLRTFactory<K> extends TreeFactory<K> {
        Object param;
        public SetInterface<K> newTree(final Object param) {
            this.param = param;
            return param.toString().isEmpty() ? new VcasHashtableDLRTAdapter()
                    : new VcasHashtableDLRTAdapter(Integer.parseInt(param.toString()));
        }
        public String getName() { return "VcasHashtableDLRT"; }
    }

    @Exclude
    protected static class VcasHashtableSteamLFFactory<K> extends TreeFactory<K> {
        Object param;
        public SetInterface<K> newTree(final Object param) {
            this.param = param;
            return param.toString().isEmpty() ? new VcasHashtableSteamLFAdapter()
                    : new VcasHashtableSteamLFAdapter(Integer.parseInt(param.toString()));
        }
        public String getName() { return "VcasHashtableSteamLF"; }
    }
    
    @Exclude
    protected static class VcasHashtableBBFFactory<K> extends TreeFactory<K> {
        Object param;
        public SetInterface<K> newTree(final Object param) {
            this.param = param;
            return param.toString().isEmpty() ? new VcasHashtableBBFAdapter()
                    : new VcasHashtableBBFAdapter(Integer.parseInt(param.toString()));
        }
        public String getName() { return "VcasHashtableBBF"; }
    }

}