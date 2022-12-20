package main.support;

import java.util.HashMap;
import java.util.stream.IntStream;

public class Zipf {
    private static final double kZipfianConst = 0.75; //99;
    private static int MAX_INT = (1<<29);
    private static HashMap<String, Double> zetaCache = new HashMap<String, Double>();
    private int items_;
    private double theta_, zeta_n_, eta_, alpha_, zeta_2_;
    private Random rng;

    public Zipf(int num_items, double zipfian_const, Random rng) {
        items_ = num_items;
        theta_ = zipfian_const;
        zeta_2_ = Zeta(2, theta_);
        zeta_n_ = Zeta(num_items, theta_);
        alpha_ = 1.0 / (1.0 - theta_);
        eta_ = Eta();
        this.rng = rng;
    }

//    public Zipf(long num_items) { this(num_items, kZipfianConst); }

    public int next() {
        double u = 1.0 * rng.nextNatural(MAX_INT) / MAX_INT; // uniform random number in [0, 1]
//        System.out.println("u: " + u);
        double uz = u * zeta_n_;
//        System.out.println(uz);
        if(uz < 1.0) return 0;
        if (uz < 1.0 + Math.pow(0.5, theta_)) return 1;
        return round((items_-1) * Math.pow(eta_ * u - eta_ + 1, alpha_));
    }

    private int round(double x) {
        return (int) (x+0.5);
    }

    private double Eta() {
        return (1 - Math.pow(2.0 / items_, 1 - theta_)) / (1 - zeta_2_ / zeta_n_);
    }

    private static double Zeta(int cur_num, double theta) {
        String key = ""+cur_num+""+theta;
        Double val = zetaCache.get(key);
        if(val != null) return val;
        val = IntStream.range(0, cur_num).parallel().mapToDouble(num -> (1.0/Math.pow(num+1, theta))).reduce(0, Double::sum);
        zetaCache.put(key, val);
        return val;
//        double sum = 0;
//        for(int i = 0; i < cur_num; i++) {
//            sum += 1.0/Math.pow(i+1, theta);
//        }
//        return sum;
    }
}
