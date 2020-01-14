package org.dataalgorithms.leftjoin.spark;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.HashSet;
import java.util.Set;

/**
 * @description:
 * @author: fanyeuxiang
 * @createDate: 2020-01-14
 */
public class SparkUseLeftJoin {

    public static void main(String[] args){
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

        JavaRDD<String> userRDD = ctx.textFile(usersInputFile, 2);
        JavaRDD<String> transactionRDD = ctx.textFile(transactionsInputFile, 2);

        JavaPairRDD<String, String> userPairRDD = userRDD.mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String s) throws Exception {
                String[] tokens = s.split(",");
                String userId = tokens[0];
                String locationId = tokens[1];
                return new Tuple2<>(userId, locationId);
            }
        });
        JavaPairRDD<String, String> transactionPairRDD = transactionRDD.mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String s) throws Exception {
                String[] tokens = s.split(",");
                String userId = tokens[2];
                String productId = tokens[1];
                return new Tuple2<>(userId, productId);
            }
        });
        JavaPairRDD<String, Tuple2<String, Optional<String>>> joinedRDD = transactionPairRDD.leftOuterJoin(userPairRDD);
        joinedRDD.collect().forEach(item ->{
            System.out.println("user_id:"+item._1 + ", (production_id, location_id):"+item._2);
        });
        joinedRDD.saveAsTextFile(outputPath+"/1");
        // 忽略user_id，创建(production_id, location_id)元组
        JavaPairRDD<String, String> productionLocationRDD = joinedRDD.mapToPair((Tuple2<String, Tuple2<String, Optional<String>>> s) ->{
            Tuple2<String, Optional<String>> pro_loca = s._2;
            String production = pro_loca._1;
            String location = pro_loca._2.get();
            return new Tuple2<>(production, location);
        });
        // groupbyKey对数据归并
        JavaPairRDD<String, Iterable<String>> productionBylocation = productionLocationRDD.groupByKey();
        // location重复值过滤与数量统计
        JavaPairRDD<String, Tuple2<Integer, Set<String>>> locationcompute = productionBylocation.mapValues((Iterable<String> locations) -> {
            Set<String> locationSet = new HashSet<>();
            locations.forEach(locationSet::add);
            return new Tuple2<>(locationSet.size(), locationSet);
        });
        locationcompute.saveAsTextFile(outputPath+"/2");
    }
}
