package org.dataalgorithms.leftjoin.spark;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @description:
 * @author: fanyeuxiang
 * @createDate: 2020-01-13
 */
public class SparkLeftOuterJoin {

    public static void main(String[] args) {
        if (args.length < 3) {
            System.err.println("Usage: SparkLeftOuterJoin <users> <transactions> <outputPath>");
            System.exit(1);
        }
        String usersInputFile = args[0];
        String transactionsInputFile = args[1];
        String outputPath = args[2];
        System.out.println("users=" + usersInputFile);
        System.out.println("transactions=" + transactionsInputFile);
        System.out.println("outputPath=" + outputPath);

        JavaSparkContext ctx = new JavaSparkContext();
        JavaRDD<String> userRDD = ctx.textFile(usersInputFile, 1);
        JavaRDD<String> transactions = ctx.textFile(transactionsInputFile, 1);

        JavaPairRDD<String, Tuple2<String, String>> userPairRDD = userRDD.mapToPair(new PairFunction<String, String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, Tuple2<String, String>> call(String s) throws Exception {
                String[] tokens = s.split(",");
                String userId = tokens[0];
                Tuple2<String, String> location = new Tuple2<String, String>("L", tokens[1]);
                return new Tuple2<String, Tuple2<String, String>>(userId, location);
            }
        });
        JavaPairRDD<String, Tuple2<String, String>> transactionsPair = transactions.mapToPair(new PairFunction<String, String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, Tuple2<String, String>> call(String s) throws Exception {
                String[] tokens = s.split(",");
                String userId = tokens[2];
                Tuple2<String, String> product = new Tuple2<String, String>("p", tokens[1]);
                return new Tuple2<String, Tuple2<String, String>>(userId, product);
            }
        });

        // here we perform a union() on usersRDD and transactionsRDD
        JavaPairRDD<String, Tuple2<String, String>> allRDD = userPairRDD.union(transactionsPair);
        // group allRDD by userID
        JavaPairRDD<String, Iterable<Tuple2<String, String>>> groupedRDD = allRDD.groupByKey();
        // PairFlatMapFunction<T, K, V>
        // T => Iterable<Tuple2<K, V>>
        JavaPairRDD<String, String> productLocationsRDD = groupedRDD.flatMapToPair(
                (Tuple2<String, Iterable<Tuple2<String, String>>> s) -> {
                    // String userID = s._1;  // NOT Needed
                    Iterable<Tuple2<String, String>> pairs = s._2;
                    String location = "UNKNOWN";
                    List<String> products = new ArrayList<String>();
                    for (Tuple2<String, String> t2 : pairs) {
                        if (t2._1.equals("L")) {
                            location = t2._2;
                        } else {
                            // t2._1.equals("P")
                            products.add(t2._2);
                        }
                    }

                    // now emit (K, V) pairs
                    List<Tuple2<String, String>> kvList = new ArrayList<Tuple2<String, String>>();
                    for (String product : products) {
                        kvList.add(new Tuple2<String, String>(product, location));
                    }
                    // Note that edges must be reciprocal, that is every
                    // {source, destination} edge must have a corresponding {destination, source}.
                    return kvList.iterator();
                }
        );
        JavaPairRDD<String, Iterable<String>> productByLocations = productLocationsRDD.groupByKey();
        JavaPairRDD<String, Tuple2<Set<String>, Integer>> productByUniqueLocations =
                productByLocations.mapValues((Iterable<String> s) -> {
                            Set<String> uniqueLocations = new HashSet<String>();
                            for (String location : s) {
                                uniqueLocations.add(location);
                            }
                            return new Tuple2<Set<String>, Integer>(uniqueLocations, uniqueLocations.size());
                        }
                );
        // debug4
        System.out.println("=== Unique Locations and Counts ===");
        List<Tuple2<String, Tuple2<Set<String>, Integer>>> debug4 = productByUniqueLocations.collect();
        System.out.println("--- debug4 begin ---");
        for (Tuple2<String, Tuple2<Set<String>, Integer>> t2 : debug4) {
            System.out.println("debug4 t2._1=" + t2._1);
            System.out.println("debug4 t2._2=" + t2._2);
        }
        System.out.println("--- debug4 end ---");
        //productByUniqueLocations.saveAsTextFile("/left/output");
        System.exit(0);
    }
}
