package org.dataalgorithms.topn.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.*;

/**
 * @description:
 *
 * Assumption: for all input (K, V), K's are unique.
 * This means that there will not etries like (A, 5) and (A, 8).
 *
 * @author: fanyeuxiang
 * @createDate: 2020-01-10
 */
public class TopN {

    public static void main(String[] args){
        // STEP-1: handle input parameters
        if (args.length < 3) {
            System.err.println("Usage: TopN <input-file><,><output-file>,<N>");
            System.exit(1);
        }
        String inputPath = args[0];
        System.out.println("args[0]: <input-path>="+inputPath);
        String outputPath = args[1];
        System.out.println("args[1]: <output-path>="+outputPath);
        Integer n = Integer.valueOf(args[2]);
        System.out.println("args[3]: <n>="+n);

        // STEP-2: create an instance of JavaSparkContext
        SparkConf conf = new SparkConf().setAppName("TopN");
        JavaSparkContext sc = new JavaSparkContext();

        JavaRDD<String> lines = sc.textFile(inputPath, 2);

        // STEP-4: create (K, V) pairs
        // Note: the assumption is that all K's are unique
        // PairFunction<T, K, V>
        // T => Tuple2<K, V>
        //
        JavaPairRDD<String, Double> pairs = lines.mapToPair(new PairFunction<String, String, Double>() {
            @Override
            public Tuple2<String, Double> call(String s) throws Exception {
                // <cat_weight><,><cat_id><,><cat_name>
                String[] tokens = s.split(",");
                return new Tuple2<String, Double>(tokens[1]+"-"+tokens[2], Double.parseDouble(tokens[0]));
            }
        });

        // STEP-5: create a local top-10
        JavaRDD<SortedMap<Double, String>> partitions = pairs.mapPartitions(new FlatMapFunction<Iterator<Tuple2<String, Double>>, SortedMap<Double, String>>() {
            @Override
            public Iterator<SortedMap<Double, String>> call(Iterator<Tuple2<String, Double>> tuple2Iterator) throws Exception {
                SortedMap<Double, String> topN = new TreeMap<Double, String>();
                while (tuple2Iterator.hasNext()){
                    Tuple2<String,Double> tuple = tuple2Iterator.next();
                    topN.put(tuple._2, tuple._1);
                    if (topN.size() > n){
                        topN.remove(topN.firstKey());
                    }
                }
                return Collections.singletonList(topN).iterator();
            }
        });

        // STEP-6: find a final top-10
        SortedMap<Double, String> finaltop10 = new TreeMap<Double, String>();
        List<SortedMap<Double, String>> alltop10 = partitions.collect();
        for (SortedMap<Double, String> localtop10 : alltop10) {
            //System.out.println(tuple._1 + ": " + tuple._2);
            // weight/count = tuple._1
            // catname/URL = tuple._2
            for (Map.Entry<Double, String> entry : localtop10.entrySet()) {
                //   System.out.println(entry.getKey() + "--" + entry.getValue());
                finaltop10.put(entry.getKey(), entry.getValue());
                // keep only top 10
                if (finaltop10.size() > n) {
                    finaltop10.remove(finaltop10.firstKey());
                }
            }
        }

        // STEP_7: emit final top-10
        for (Map.Entry<Double, String> entry : finaltop10.entrySet()) {
            System.out.println(entry.getKey() + "--" + entry.getValue());
        }

        System.exit(0);
    }

}
