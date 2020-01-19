package org.dataalgorithms.MBA.spark;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.dataalgorithms.MBA.mapreduce.Combination;
import scala.Tuple2;
import scala.Tuple3;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

/**
 * @description: 购物蓝关联规则挖
 * @author: fanyeuxiang
 * @createDate: 2020-01-17
 */
public class FindAssociationRules {

    public static void main(String[] args){
        if (args.length != 3) {
            System.out.println("USAGE: [input-path] [output-path] [number-of-pairs]");
            System.exit(1);
        }

        String inputPath = args[0];
        String outputPath = args[1];
        int windowSize = Integer.parseInt(args[2]);
        if (windowSize < 0) {
            System.out.println("windowSize must bigger than 0");
            System.exit(1);
        }
        JavaSparkContext ctx = new JavaSparkContext();
        JavaRDD<String> lines = ctx.textFile(inputPath);
        // 第一步：生成频繁模式
        JavaPairRDD<List<String>, Integer> windowPairs = lines.flatMapToPair((String transaction)->{
            List<String> productions = Arrays.asList(transaction.split(","));
            //Combination 方案避免元组<a, b>与<b, a>这样的重复
            List<List<String>> combinations = Combination.findSortedCombinations(productions);
            List<Tuple2<List<String>, Integer>> result = new ArrayList<>();
            for (List<String> combination : combinations){
                if (combination.size() > 0){
                    result.add(new Tuple2<>(combination, 1));
                }
            }
            return result.iterator();
        });
        windowPairs.saveAsTextFile(outputPath + "/1");
        // 组合归约频繁模式
        JavaPairRDD<List<String>, Integer> combined = windowPairs.reduceByKey((Integer i1, Integer i2) -> i1 + i2);
        combined.saveAsTextFile(outputPath + "/2");
        /**
         * 生成所有子模式
         * now, we have: patterns(K,V) K = pattern as List<String>  V = frequency of pattern
         * now given (K,V) as (List<a,b,c>, 2) we will
         * generate the following (K2,V2) pairs，K2是K的子集
         * (List<a,b,c>, T2(null, 2))
         * (List<a,b>,   T2(List<a,b,c>, 2))
         * (List<a,c>,   T2(List<a,b,c>, 2))
         * (List<b,c>,   T2(List<a,b,c>, 2))
         */
        JavaPairRDD<List<String>, Tuple2<List<String>, Integer>> subpatterns = combined.flatMapToPair((Tuple2<List<String>, Integer> pattern) ->{
            // 所有子模式集合
            List<Tuple2<List<String>,Tuple2<List<String>,Integer>>> result = new LinkedList<>();
            Integer frequency = pattern._2;
            List<String> list = pattern._1;
            result.add(new Tuple2(list, new Tuple2(null,frequency)));
            if (list.size() == 1) {
                return result.iterator();
            }
            // pattern has more than one items
            // result.add(new Tuple2(list, new Tuple2(null,size)));
            for (int i = 0; i < list.size(); i++){
                result.add(new Tuple2<>(CollectionsUtil.removeOneItem(list, i), new Tuple2<>(list, frequency)));
            }
            return result.iterator();
        });
        subpatterns.saveAsTextFile(outputPath + "/3");
        // 组合子模式，groupBy
        JavaPairRDD<List<String>, Iterable<Tuple2<List<String>, Integer>>> rules = subpatterns.groupByKey();
        rules.saveAsTextFile(outputPath + "/4");
        /**
         * 生成关联规则
         * Now, use (K=List<String>, V=Iterable<Tuple2<List<String>,Integer>>) to generate association rules
         * JavaRDD<R> map(Function<T,R> f)
         * Return a new RDD by applying a function to all elements of this RDD.
         * T: input
         * R: ( ac => b, 1/3): T3(List(a,c), List(b),  0.33)
         *    ( ad => c, 1/3): T3(List(a,d), List(c),  0.33)
         */
        JavaRDD<List<Tuple3<List<String>,List<String>,Double>>> assocRules = rules.map((Tuple2<List<String>,Iterable<Tuple2<List<String>,Integer>>> in) ->{
            List<Tuple3<List<String>,List<String>,Double>> result =
                    new ArrayList<Tuple3<List<String>,List<String>,Double>>();
            List<String> fromList = in._1;
            Iterable<Tuple2<List<String>,Integer>> to = in._2;
            Tuple2<List<String>,Integer> fromCount = null;
            List<Tuple2<List<String>,Integer>> toList = new ArrayList<Tuple2<List<String>,Integer>>();
            for (Tuple2<List<String>,Integer> t2 : to) {
                // find the "count" object
                if (t2._1 == null) {
                    fromCount = t2;
                }
                else {
                    toList.add(t2);
                }
            }
            // Now, we have the required objects for generating association rules:
            //  "fromList", "fromCount", and "toList"
            if (toList.isEmpty()) {
                // no output generated, but since Spark does not like null objects, we will fake a null object
                return result; // an empty list
            }
            // now using 3 objects: "from", "fromCount", and "toList",
            // create association rules:
            for (Tuple2<List<String>,Integer>  t2 : toList) {
                double confidence = (double) t2._2 / (double) fromCount._2;
                List<String> t2List = new ArrayList<String>(t2._1);
                t2List.removeAll(fromList);
                result.add(new Tuple3(fromList, t2List, confidence));
            }
            return result;
        });
        assocRules.saveAsTextFile(outputPath + "/5");
        // done
        ctx.close();
        System.exit(0);
    }

}
