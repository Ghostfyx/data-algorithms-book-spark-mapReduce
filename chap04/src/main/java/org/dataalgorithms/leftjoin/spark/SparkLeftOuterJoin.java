package org.dataalgorithms.leftjoin.spark;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Iterator;

/**
 * @description:
 * @author: fanyeuxiang
 * @createDate: 2020-01-13 19:51
 */
public class SparkLeftOuterJoin {

    public static void main(String[] args){
        if (args.length < 2) {
            System.err.println("Usage: SparkLeftOuterJoin <users> <transactions>");
            System.exit(1);
        }
        String usersInputFile = args[0];
        String transactionsInputFile = args[1];
        System.out.println("users="+ usersInputFile);
        System.out.println("transactions="+ transactionsInputFile);

        JavaSparkContext ctx = new JavaSparkContext();
        JavaRDD<String> userRDD = ctx.textFile(usersInputFile, 1);
        JavaRDD<String> transactions = ctx.textFile(transactionsInputFile, 1);

        JavaPairRDD<String, Tuple2<String, String>> userPairRDD = userRDD.mapToPair(new PairFunction<String, String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, Tuple2<String, String>> call(String s) throws Exception {
                String[] tokens = s.split(",");
                String userId = tokens[0];
                Tuple2<String,String> location = new Tuple2<String,String>("L", tokens[1]);
                return new Tuple2<String, Tuple2<String, String>>(userId, location);
            }
        });
        JavaPairRDD<String, Tuple2<String, String>> transactionsPair = transactions.mapToPair(new PairFunction<String, String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, Tuple2<String, String>> call(String s) throws Exception {
                String[] tokens = s.split(",");
                String userId = tokens[2];
                Tuple2<String,String> product = new Tuple2<String,String>("p", tokens[1]);
                return new Tuple2<String, Tuple2<String, String>>(userId, product);
            }
        });

        // here we perform a union() on usersRDD and transactionsRDD
        JavaPairRDD<String,Tuple2<String,String>> allRDD = userPairRDD.union(transactionsPair);
        // group allRDD by userID
        JavaPairRDD<String, Iterable<Tuple2<String,String>>> groupedRDD = allRDD.groupByKey();
        JavaPairRDD<String, Integer> productLocationsRDD = groupedRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Iterable<Tuple2<String, String>>>, String, Integer>() {
            @Override
            public Iterator<Tuple2<String, Integer>> call(Tuple2<String, Iterable<Tuple2<String, String>>> stringIterableTuple2) throws Exception {
                Iterable<Tuple2<String,String>> pairs = stringIterableTuple2._2;
                String location = "UNKNOWN";

                return null;
            }
        });
    }

}
