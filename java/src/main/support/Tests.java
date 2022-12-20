/**
 * Java test harness for throughput experiments on concurrent data structures.
 * Copyright (C) 2012 Trevor Brown
 * Copyright (C) 2019 Elias Papavasileiou
 * Contact (me [at] tbrown [dot] pro) with any questions or comments.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package main.support;

import adapters.*;
import algorithms.common.Camera;
import algorithms.hashtable.VcasHashtableEpoch;

import java.util.*;

public class Tests {

	/*static class MyInsertThread extends Thread {
		private LockFreePBSTAdapter<Integer> tree;

		public MyInsertThread(LockFreePBSTAdapter<Integer> tree) {
			this.tree = tree;
		}

		public void run() {
			tree.add(10, null);
		}
	}

	static class MyDeleteThread extends Thread {
		private LockFreePBSTAdapter<Integer> tree;

		public MyDeleteThread(LockFreePBSTAdapter<Integer> tree) {
			this.tree = tree;
		}

		public void run() {
			for (int i=0; i<3; i++) {
				tree.remove(10, null);
			}
		}
	}*/

    static boolean validationFailed = false;

    static void validateTree(AbstractAdapter<Integer> tree) {
//        if(!validationFailed && tree instanceof Profileable) {
//            Profileable profTree = (Profileable) tree;
//            int maxVerListLength = profTree.maxVersionListLengths();
//            int doublyLinkedMinusSinglyLinked = profTree.doublyLinkedVsSinglyLinked();
////            if(maxVerListLength != 1 && !(tree instanceof VcasChromaticBBFAdapter)) {
////                System.out.println("====ERROR===== Max version list length: " + maxVerListLength);
////                validationFailed = true;
////            }
////            if(doublyLinkedMinusSinglyLinked != 0) {
////                System.out.println("====ERROR===== Doubly linked larger than singly linked by: " + doublyLinkedMinusSinglyLinked);
////                validationFailed = true;
////            }
//        }
    }

    static void InsertDeleteOneKey(AbstractAdapter<Integer> tree) {
        validateTree(tree);
        assert tree.add(10, null);
        validateTree(tree);
        assert !tree.add(10, null);
        validateTree(tree);
        assert tree.remove(10, null);
        validateTree(tree);
        assert !tree.remove(10, null);
        validateTree(tree);
        System.out.println(new Object(){}.getClass().getEnclosingMethod().getName() + ": OK");
        validationFailed = false;
    }

    static void InsertDeleteTwoKeys(AbstractAdapter<Integer> tree) {
        validateTree(tree);
        assert tree.add(10, null);
        validateTree(tree);
        assert tree.add(15, null);
        validateTree(tree);
        assert !tree.add(10, null);
        validateTree(tree);
//        System.out.println(tree.toString());
//        boolean result = !tree.add(15, null);
//        System.out.println(tree.toString());
        assert !tree.add(15, null);
        validateTree(tree);
        assert tree.remove(10, null);
        validateTree(tree);
        assert !tree.remove(10, null);
        validateTree(tree);
        assert tree.remove(15, null);
        validateTree(tree);
        assert !tree.remove(15, null);
        validateTree(tree);
        System.out.println(new Object(){}.getClass().getEnclosingMethod().getName() + ": OK");
        validationFailed = false;
    }

    static void RandomInsertDelete(AbstractAdapter<Integer> tree) {
        validateTree(tree);
        tree.add(7, null);
        validateTree(tree);
        tree.add(8, null);
        validateTree(tree);
        tree.add(1, null);
        validateTree(tree);
        tree.remove(1, null);
        validateTree(tree);
        tree.add(2, null);
        validateTree(tree);
        tree.remove(8, null);
        validateTree(tree);
        tree.add(1, null);
        validateTree(tree);
        tree.add(8, null);
        validateTree(tree);
        tree.add(5, null);
        validateTree(tree);
        tree.add(3, null);
        validateTree(tree);
        tree.add(4, null);
        validateTree(tree);
        tree.remove(4, null);
        validateTree(tree);
        tree.remove(1, null);
        validateTree(tree);
        tree.remove(2, null);
        validateTree(tree);
        tree.remove(7, null);
        validateTree(tree);
        tree.add(4, null);
        validateTree(tree);
        tree.add(1, null);
        tree.remove(1, null);
        tree.remove(4, null);
        tree.remove(3, null);
        tree.remove(8, null);
        tree.remove(5, null);
        validateTree(tree);
        System.out.println(new Object(){}.getClass().getEnclosingMethod().getName() + ": OK");
        validationFailed = false;
    }

    static void InsertDeleteQuery(AbstractAdapter<Integer> tree) {
        validateTree(tree);
        try {
            tree.rangeQuery(1, 100, 0, null);
        } catch(UnsupportedOperationException e) {
            System.out.println("range query not supported");
            return;
        }
        validateTree(tree);
        int keysInTree = 0;
        int key1 = 11, key2 = 20, key3 = 30;
        int rqResult = ((Object[]) tree.rangeQuery(1, 100, 0, null)).length;
        assert rqResult == keysInTree;
        validateTree(tree);
        assert !tree.remove(key1, null);
        validateTree(tree);
        assert tree.add(key1, null);
        validateTree(tree);
        keysInTree++;
        assert tree.add(key3, null);
        validateTree(tree);
        keysInTree++;
        assert tree.add(key2, null);
        validateTree(tree);
        keysInTree++;
        // System.out.println("RQResult length: " + ((Object[]) tree.rangeQuery(1, 100, 0, null)).length);
        // System.out.println("keysInTree: " + keysInTree);
//        System.out.println(tree.toString());
//        System.out.println(((Object[]) tree.rangeQuery(1, 100, 0, null))[0]);
        assert ((Object[]) tree.rangeQuery(1, 100, 100, null)).length == keysInTree;
        validateTree(tree);
        //System.out.println("RQResult length: " + ((Object[]) tree.rangeQuery(1, 100, 0, null)).length);
        //assert tree.remove(key1, null);
        //assert !tree.contains(key1);
        assert tree.remove(key1, null);
        validateTree(tree);
        keysInTree--;
        assert !tree.contains(key1);
        validateTree(tree);
        // Object[] keys = (Object[]) tree.rangeQuery(1, 100, 0, null);
        // System.out.println("RQResult length: " + keys.length);
        // System.out.println("keysInTree: " + keysInTree);
        // for(int i = 0; i < keys.length; i++)
        //     System.out.print((Integer) keys[i] + ", ");
        // System.out.println();
        assert ((Object[]) tree.rangeQuery(1, 100, 0, null)).length == keysInTree;
        validateTree(tree);
        assert !tree.contains(key1);
        assert tree.contains(key2);
        assert tree.contains(key3);
        assert !tree.remove(key1, null);
        validateTree(tree);
        assert tree.contains(key2);
        assert tree.contains(key3);
        assert ((Object[]) tree.rangeQuery(key2, key3-1, 0, null)).length == keysInTree-1;
        validateTree(tree);
        assert ((Object[]) tree.rangeQuery(key2, key3, 0, null)).length == keysInTree;
        validateTree(tree);
        assert ((Object[]) tree.rangeQuery(key2, key3+1, 0, null)).length == keysInTree;
        validateTree(tree);
        assert tree.remove(key2, null);
        validateTree(tree);
        keysInTree--;
        assert tree.remove(key3, null);
        validateTree(tree);
        keysInTree--;
        System.out.println(new Object(){}.getClass().getEnclosingMethod().getName() + ": OK");
        validationFailed = false;
    }

    static void InsertDeleteQuery2(AbstractAdapter<Integer> tree) {
        validateTree(tree);
        try {
            tree.rangeQuery(1, 100, 0, null);
        } catch(UnsupportedOperationException e) {
            System.out.println("range query not supported");
            return;
        }
        validateTree(tree);
        int keysInTree = 0;
        assert ((Object[]) tree.rangeQuery(1, 100, 0, null)).length == keysInTree;
        validateTree(tree);
        assert !tree.contains(10);
        assert ((Object[]) tree.rangeQuery(1, 100, 0, null)).length == keysInTree;
        validateTree(tree);
        assert tree.add(10, null);
        validateTree(tree);
        keysInTree++;
        assert tree.add(15, null);
        validateTree(tree);
        keysInTree++;
        assert ((Object[]) tree.rangeQuery(1, 100, 0, null)).length == keysInTree;
        validateTree(tree);
        assert tree.remove(15, null);
        validateTree(tree);
        keysInTree--;
        assert !tree.remove(15, null);
        validateTree(tree);
        assert ((Object[]) tree.rangeQuery(1, 100, 0, null)).length == keysInTree;
        validateTree(tree);
        assert !tree.contains(15);
		assert ((Object[]) tree.rangeQuery(1, 1000000, 0, null)).length == keysInTree;
        validateTree(tree);
        assert tree.add(5, null);
        validateTree(tree);
        keysInTree++;
        assert tree.remove(5, null);
        validateTree(tree);
        keysInTree--;
        assert ((Object[]) tree.rangeQuery(1, 100, 0, null)).length == keysInTree;
        validateTree(tree);
		assert tree.add(20, null);
        validateTree(tree);
        keysInTree++;
		assert tree.add(15, null);
        validateTree(tree);
        keysInTree++;
		assert tree.add(25, null);
        validateTree(tree);
        keysInTree++;
		assert !tree.remove(5, null);
        validateTree(tree);
		assert ((Object[]) tree.rangeQuery(1, 100, 0, null)).length == keysInTree;
        validateTree(tree);
		assert tree.remove(25, null);
        validateTree(tree);
        keysInTree--;
		assert ((Object[]) tree.rangeQuery(1, 100, 0, null)).length == keysInTree;
        assert ((Object[]) tree.rangeQuery(1, 10, 0, null)).length == keysInTree-2;
        assert ((Object[]) tree.rangeQuery(1, 14, 0, null)).length == keysInTree-2;
        assert ((Object[]) tree.rangeQuery(1, 15, 0, null)).length == keysInTree-1;
        assert ((Object[]) tree.rangeQuery(1, 16, 0, null)).length == keysInTree-1;
        assert ((Object[]) tree.rangeQuery(9, 16, 0, null)).length == keysInTree-1;
        assert ((Object[]) tree.rangeQuery(10, 16, 0, null)).length == keysInTree-1;
        assert ((Object[]) tree.rangeQuery(11, 16, 0, null)).length == keysInTree-2;
        validateTree(tree);
//        System.out.println(tree.toString());
        tree.remove(15, null);
        tree.remove(20, null);
        tree.remove(10, null);
//        System.out.println(tree.toString());
//        System.out.println("size = " + tree.size());
        System.out.println(new Object(){}.getClass().getEnclosingMethod().getName() + ": OK");
        validationFailed = false;
    }

    public static class InsertThread extends Thread {
        ArrayList<Integer> keys;
        AbstractAdapter<Integer> tree;

        public InsertThread(AbstractAdapter<Integer> tree, ArrayList<Integer> keys) {
            this.tree = tree;
            this.keys = keys;
        }

        public void run(){
            ThreadID.threadID.set(1);
            for(int i = 0; i < keys.size(); i++)
                assert tree.add(keys.get(i), null);
        }
    }

    // You can test snapshots in 2 ways: have a long snapshot concurrent with updates. Keys are selected randomly where as values are always increasing
    // You can also split the snapshot operation into a take snapshot and range query.
    static void ConcurrentInsertQuery(AbstractAdapter<Integer> tree) {
        try {
            tree.rangeQuery(1, 100, 0, null);
        } catch(UnsupportedOperationException e) {
            System.out.println("range query not supported");
            return;
        }

        int num_retries = 8;
        for(int ii = 0; ii < num_retries; ii++) {
            int treeSize = 1000000;
            int numQueries = 3;
//        int treeSize = 20;
//        int numQueries = 1;
            ArrayList<Integer> oddKeys = new ArrayList<Integer>();
            for(int i = 0; i < treeSize; i++)
                oddKeys.add(2*i+1);
            Collections.shuffle(oddKeys);
            ArrayList<Integer> evenKeys = new ArrayList<Integer>();
            for(int i = 0; i < treeSize; i++)
                evenKeys.add(2*i);
            Collections.shuffle(evenKeys);
            for(int i = 0; i < treeSize; i++)
                assert tree.add(evenKeys.get(i), null);
            int[] location = new int[2*treeSize];
            for(int i = 0; i < oddKeys.size(); i++) {
                location[oddKeys.get(i)] = i;
//            System.out.print(oddKeys.get(i) + " ");
            }
//        System.out.println();

            InsertThread insertThread = new InsertThread(tree, oddKeys);
            insertThread.start();
            boolean intermediateValueSpotted = false;
            while(insertThread.isAlive()) {
                Object[] keysInRange = (Object[]) tree.rangeQuery(-1, 2*treeSize+2, 2*treeSize+3, null);
                boolean[] seen = new boolean[treeSize];
//            for(int i = 0; i < treeSize; i++) seen[i] = false;
                int max = -1;

                for(Object o : keysInRange) {
                    Integer i = (Integer) o;
//                System.out.println("KEY: " + i);
                    if (i % 2 == 1) {
                        max = Math.max(max, location[i]);
//                    System.out.println("max: " + max);
                        seen[location[i]] = true;
                    }
                }

//            System.out.println(tree.toString());

//            Object[] keysInRange2 = (Object[]) ((VcasHashtableEpochAdapter)tree).ds.rangeScanNonAtomic(-1, 2*treeSize+2);
//            boolean[] seen2 = new boolean[treeSize];
////            for(int i = 0; i < treeSize; i++) seen[i] = false;
//            int max2 = -1;
//
//            for(Object o : keysInRange2) {
//                Integer i = (Integer) o;
////                System.out.println("KEY: " + i);
//                if (i % 2 == 1) {
//                    max2 = Math.max(max2, location[i]);
////                    System.out.println("max: " + max);
//                    seen2[location[i]] = true;
//                }
//            }

//            for(int i = 0; i <= max; i++)
//                System.out.print((seen[i] ? 1 : 0));
//            System.out.println();
//            for(int i = 0; i <= max2; i++)
//                System.out.print((seen2[i] ? 1 : 0));
//            System.out.println();
                for(int i = 0; i <= max; i++) {
//                System.out.println(i);
                    assert seen[i];
                }
//            System.out.println("max: " + max);
                if(max > 0 && max < treeSize-2) {
                    intermediateValueSpotted = true;
                }
//            System.out.println("passed concurrent insert query test with max = " + max);
            }

//        Object[] keysInRange = (Object[]) tree.rangeQuery(-1, 2*treeSize+2, 2*treeSize+3, null);
//        boolean[] seen = new boolean[treeSize];
////            for(int i = 0; i < treeSize; i++) seen[i] = false;
//        int max = -1;
//
//        for(Object o : keysInRange) {
//            Integer i = (Integer) o;
////                System.out.println("KEY: " + i);
//            if (i % 2 == 1) {
//                max = Math.max(max, location[i]);
////                    System.out.println("max: " + max);
//                seen[location[i]] = true;
//            }
//        }
//
//        for(int i = 0; i <= max; i++)
//            System.out.print((seen[i] ? 1 : 0));
//        System.out.println();
//
//        Object[] keysInRange2 = (Object[]) ((VcasHashtableEpochAdapter)tree).ds.rangeScanNonAtomic(-1, 2*treeSize+2);
//        boolean[] seen2 = new boolean[treeSize];
////            for(int i = 0; i < treeSize; i++) seen[i] = false;
//        int max2 = -1;
//
//        for(Object o : keysInRange2) {
//            Integer i = (Integer) o;
////                System.out.println("KEY: " + i);
//            if (i % 2 == 1) {
//                max2 = Math.max(max2, location[i]);
////                    System.out.println("max: " + max);
//                seen2[location[i]] = true;
//            }
//        }
//
//        for(int i = 0; i <= max2; i++)
//            System.out.print((seen2[i] ? 1 : 0));
//        System.out.println();

            for(int i = 0; i < 2*treeSize; i++) {
                if(!tree.remove(i, null)) {
                    System.out.println("failed to remove: " + i);
                    assert false;
                }
            }

            if(intermediateValueSpotted) break;
            else if(ii == num_retries-1) assert false;
        }
        tree.cleanup();
        System.out.println(new Object(){}.getClass().getEnclosingMethod().getName() + ": OK");
    }



    static void InsertDeleteOneKeyKiwi(AbstractAdapter<Integer> tree) {
        tree.add(10, null);
        assert tree.contains(10);
        tree.add(10, null);
        assert tree.contains(10);
        assert !tree.contains(11);
        tree.remove(10, null);
        assert !tree.contains(10);
        tree.remove(10, null);
        assert !tree.contains(10);
        System.out.println(new Object(){}.getClass().getEnclosingMethod().getName() + ": OK");
    }

    static void InsertDeleteTwoKeysKiwi(AbstractAdapter<Integer> tree) {
        tree.add(10, null);
        assert tree.contains(10);
        tree.add(15, null);
        assert tree.contains(15);
        tree.add(10, null);
        tree.add(15, null);
        tree.remove(10, null);
        assert !tree.contains(10);
        assert tree.contains(15);
        tree.remove(10, null);
        tree.remove(15, null);
        assert !tree.contains(15);
        tree.remove(15, null);
        System.out.println(new Object(){}.getClass().getEnclosingMethod().getName() + ": OK");
    }

    static void InsertDeleteQueryKiwi(AbstractAdapter<Integer> tree) {
        try {
            tree.rangeQuery(1, 100, 0, null);
        } catch(UnsupportedOperationException e) {
            System.out.println("range query not supported");
            return;
        }
        int keysInTree = 0;
        int key1 = 11, key2 = 20, key3 = 30;
        assert ((Object[]) tree.rangeQuery(1, 100, 0, null)).length == keysInTree;
        tree.remove(key1, null);
        assert !tree.contains(key1);
        tree.add(key1, null);
        assert tree.contains(key1);
        keysInTree++;
        tree.add(key3, null);
        assert tree.contains(key3);
        keysInTree++;
        tree.add(key2, null);
        assert tree.contains(key2);
        keysInTree++;
        // System.out.println("RQResult length: " + ((Object[]) tree.rangeQuery(1, 100, 0, null)).length);
        // System.out.println("keysInTree: " + keysInTree);
        assert ((Object[]) tree.rangeQuery(1, 100, 0, null)).length == keysInTree;
        //System.out.println("RQResult length: " + ((Object[]) tree.rangeQuery(1, 100, 0, null)).length);
        //assert tree.remove(key1, null);
        //assert !tree.contains(key1);
        tree.remove(key1, null);
        keysInTree--;
        assert !tree.contains(key1);
        //System.out.println("RQResult length: " + ((Object[]) tree.rangeQuery(1, 100, 0, null)).length);
        assert ((Object[]) tree.rangeQuery(1, 100, 0, null)).length == keysInTree;
        assert !tree.contains(key1);
        assert tree.contains(key2);
        assert tree.contains(key3);
        tree.remove(key1, null);
        assert tree.contains(key2);
        assert tree.contains(key3);
        assert ((Object[]) tree.rangeQuery(key2, key3-1, 0, null)).length == keysInTree-1;
        assert ((Object[]) tree.rangeQuery(key2, key3, 0, null)).length == keysInTree;
        assert ((Object[]) tree.rangeQuery(key2, key3+1, 0, null)).length == keysInTree;
        tree.remove(key2, null);
        assert !tree.contains(key2);
        keysInTree--;
        tree.remove(key3, null);
        assert !tree.contains(key3);
        keysInTree--;
        System.out.println(new Object(){}.getClass().getEnclosingMethod().getName() + ": OK");
    }

    static void InsertDeleteQuery2Kiwi(AbstractAdapter<Integer> tree) {
        try {
            tree.rangeQuery(1, 100, 0, null);
        } catch(UnsupportedOperationException e) {
            System.out.println("range query not supported");
            return;
        }
        int keysInTree = 0;
        assert ((Object[]) tree.rangeQuery(1, 100, 0, null)).length == keysInTree;
        assert !tree.contains(10);
        assert ((Object[]) tree.rangeQuery(1, 100, 0, null)).length == keysInTree;
        tree.add(10, null);
        keysInTree++;
        tree.add(15, null);
        keysInTree++;
        assert ((Object[]) tree.rangeQuery(1, 100, 0, null)).length == keysInTree;
        tree.remove(15, null);
        keysInTree--;
        assert ((Object[]) tree.rangeQuery(1, 100, 0, null)).length == keysInTree;
        assert !tree.contains(15);
        assert ((Object[]) tree.rangeQuery(1, 1000000, 0, null)).length == keysInTree;
        tree.add(5, null);
        keysInTree++;
        tree.remove(5, null);
        keysInTree--;
        assert ((Object[]) tree.rangeQuery(1, 100, 0, null)).length == keysInTree;
        tree.add(20, null);
        keysInTree++;
        tree.add(15, null);
        keysInTree++;
        tree.add(25, null);
        keysInTree++;
        tree.remove(5, null);
        assert ((Object[]) tree.rangeQuery(1, 100, 0, null)).length == keysInTree;
        tree.remove(25, null);
        keysInTree--;
        assert ((Object[]) tree.rangeQuery(1, 100, 0, null)).length == keysInTree;
        assert ((Object[]) tree.rangeQuery(1, 10, 0, null)).length == keysInTree-2;
        assert ((Object[]) tree.rangeQuery(1, 14, 0, null)).length == keysInTree-2;
        assert ((Object[]) tree.rangeQuery(1, 15, 0, null)).length == keysInTree-1;
        assert ((Object[]) tree.rangeQuery(1, 16, 0, null)).length == keysInTree-1;
        assert ((Object[]) tree.rangeQuery(9, 16, 0, null)).length == keysInTree-1;
        assert ((Object[]) tree.rangeQuery(10, 16, 0, null)).length == keysInTree-1;
        assert ((Object[]) tree.rangeQuery(11, 16, 0, null)).length == keysInTree-2;
        System.out.println(new Object(){}.getClass().getEnclosingMethod().getName() + ": OK");
    }

    private static void testVcasHashtableEpoch(VcasHashtableEpoch<Integer, Integer> tree) {
//        System.out.println(tree.toString());
        long ts = Camera.takeSnapshot();
//        System.out.println("ts = " + ts + ", " + tree.toString(ts));
        assert tree.putIfAbsent(1000, 1000);
//        System.out.println(tree.toString());
        long ts1 = Camera.takeSnapshot();
        assert tree.putIfAbsent(15, 10);
//        System.out.println(tree.toString());
        long ts2 = Camera.takeSnapshot();
        assert !tree.putIfAbsent(1000, 1000);
//        System.out.println(tree.toString());
//        System.out.println("ts = " + ts + ", " + tree.toString(ts));
//        System.out.println(tree.toString());
//        boolean result = !tree.add(15, null);
//        System.out.println(tree.toString());
        assert !tree.putIfAbsent(15, 10);
        assert tree.remove(1000);
        long ts3 = Camera.takeSnapshot();
        assert !tree.remove(1000);
        assert tree.remove(15);
        long ts4 = Camera.takeSnapshot();
        assert !tree.remove(15);
//        System.out.println("ts1 = " + ts1 + ", " + tree.toString(ts1));
//        System.out.println("ts2 = " + ts2 + ", " + tree.toString(ts2));
//        System.out.println("ts3 = " + ts3 + ", " + tree.toString(ts3));
//        System.out.println("ts4 = " + ts4 + ", " + tree.toString(ts4));

        System.out.println(new Object(){}.getClass().getEnclosingMethod().getName() + ": OK");
        validationFailed = false;
    }

    private static void runTests(AbstractAdapter<Integer> tree, boolean testLinearRangeQuery) {
        InsertDeleteOneKey(tree);
        InsertDeleteTwoKeys(tree);
        RandomInsertDelete(tree);
        InsertDeleteQuery(tree);
        InsertDeleteQuery2(tree);
        if(tree instanceof VcasHashtableEpochAdapter) {
            testVcasHashtableEpoch(((VcasHashtableEpochAdapter)tree).ds);
        }
        if(tree.supportsLinearizableRangeQuery() && testLinearRangeQuery)
            ConcurrentInsertQuery(tree);
    }

    private static void runKiwiTests(AbstractAdapter<Integer> tree) {
        InsertDeleteOneKeyKiwi(tree);
        InsertDeleteTwoKeysKiwi(tree);
        InsertDeleteQueryKiwi(tree);
        InsertDeleteQuery2Kiwi(tree);
    }

    public static void runTests() {
        int[] treeParam = {6};
        ThreadID.threadID.set(0);

        // Run tests
        for (TreeFactory<Integer> tree : Factories.factories) {
//            if(tree.getName() == "VcasChromaticBatchBSTBaseline")
//                continue; // TODO: make sure to clear the announcement array after running range queries on Baseline
            if(tree.getName().indexOf("Hashtable") != -1) {
                System.out.println("[*] Testing " + tree.getName() + " 1 bucket...");
                AbstractAdapter<Integer> treeAdapter = (AbstractAdapter<Integer>) tree.newTree(1);
                runTests(treeAdapter, false);
                System.out.println();

                System.out.println("[*] Testing " + tree.getName() + " 10M buckets...");
                treeAdapter = (AbstractAdapter<Integer>) tree.newTree(1000000);
                runTests(treeAdapter, true);
                System.out.println();
            }
            else {
                System.out.println("[*] Testing " + tree.getName() + " ...");
                AbstractAdapter<Integer> treeAdapter = (AbstractAdapter<Integer>) tree.newTree("");
                runTests(treeAdapter, true);
                System.out.println();
            }          
        }        
    }
}
