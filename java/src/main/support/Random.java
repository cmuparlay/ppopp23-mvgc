/**
 * Java test harness for throughput experiments on concurrent data structures.
 * Copyright (C) 2012 Trevor Brown
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

import org.deuce.transform.Exclude;

import java.util.concurrent.ThreadLocalRandom;
import jdk.internal.vm.annotation.Contended;

@Exclude
public class Random {
    @Contended
    private int seed;

    public Random(int seed) {
        this.seed = seed;
    }

    /** returns pseudorandom x satisfying 0 <= x < n. **/
    public int nextNatural(int n) {
        seed ^= seed << 6;
        seed ^= seed >>> 21;
        seed ^= seed << 7;
        return (seed % n < 0 ? -(seed % n) : seed % n);
    }

    /** returns pseudorandom x satisfying 0 <= x < MAX_INT. **/
    public int nextNatural() {
        seed ^= seed << 6;
        seed ^= seed >>> 21;
        seed ^= seed << 7;
        return (seed < 0 ? -seed : seed);
    }

    /** returns pseudorandom x satisfying MIN_INT <= x <= MAX_INT. **/
    public int nextInt() {
        seed ^= seed << 6;
        seed ^= seed >>> 21;
        seed ^= seed << 7;
        return seed;
    }

    // Implementing FisherYates shuffle
    // source: https://stackoverflow.com/questions/1519736/random-shuffling-of-an-array
    public static void shuffleArray(int[] ar) {
        // If running on Java 6 or older, use `new Random()` on RHS here
        java.util.Random rnd = ThreadLocalRandom.current();
        for (int i = ar.length - 1; i > 0; i--) {
            int index = rnd.nextInt(i + 1);
            // Simple swap
            int a = ar[index];
            ar[index] = ar[i];
            ar[i] = a;
        }
    }
}
