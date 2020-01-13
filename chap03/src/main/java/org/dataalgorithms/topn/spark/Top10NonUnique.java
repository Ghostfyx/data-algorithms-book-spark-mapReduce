package org.dataalgorithms.topn.spark;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.*;

/**
 * @description:
 * @author: fanyeuxiang
 * @createDate: 2020-01-13
 */
public class Top10NonUnique {

    public static void main(String[] args) throws Exception {
        // STEP-1: handle input parameters
        if (args.length < 3) {
            System.err.println("Usage: Top10 <input-path> <out-path> <topN>");
            System.exit(1);
        }
        System.out.println("args[0]: <input-path>="+args[0]);
        System.out.println("args[1]: <out-path>="+args[1]);
        System.out.println("args[2]: <topN>="+args[2]);
        final int N = Integer.parseInt(args[2]);

        // STEP-2: create a Java Spark Context object
        JavaSparkContext context = new JavaSparkContext();

        // STEP-3: broadcast the topN to all cluster nodes
       final Broadcast<Integer> topN = context.broadcast(N);

        // STEP-4: create an RDD from input
        //    input record format:
        //        <string-key><,><integer-value-count>
        JavaRDD<String> lines = context.textFile(args[0], 1);
        lines.saveAsTextFile(args[1]+"/1");
        // STEP-5: partition RDD
        // public JavaRDD<T> coalesce(int numPartitions)
        // Return a new RDD that is reduced into numPartitions partitions.
        // coalesce 函数对RDD重新分区,将 RDD 中的分区数减少为 numPartitions。
        JavaRDD<String> re_RDD = lines.coalesce(9);

        // STEP-6: map input(T) into (K,V) pair
        // PairFunction<T, K, V>
        // T => Tuple2<K, V>
        JavaPairRDD<String, Integer> kv = re_RDD.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
               String[] tokens = s.split(",");
               return new Tuple2<String, Integer>(tokens[0], Integer.parseInt(tokens[1]));
            }
        });
        kv.saveAsTextFile(args[1] + "/2");
        // STEP-7: reduce frequent K's
        JavaPairRDD<String, Integer> uniqueKeys = kv.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1+v2;
            }
        });
        uniqueKeys.saveAsTextFile(args[1]+"/3");
        // STEP-8: create a local top-N
        // 注意：当网站出现次数相同，使用Map集合作为中间结果，会出现put(key, value)，value被替换的事情
        JavaRDD<SortedMap<Integer, String>> partitions_topN = uniqueKeys.mapPartitions(new FlatMapFunction<Iterator<Tuple2<String, Integer>>, SortedMap<Integer, String>>() {
            @Override
            public Iterator<SortedMap<Integer, String>> call(Iterator<Tuple2<String, Integer>> values) throws Exception {
                SortedMap<Integer, String> localTopMap = new TreeMap<>();
                while (values.hasNext()){
                    Tuple2<String, Integer> value = values.next();
                    localTopMap.put(value._2, value._1);
                    if (localTopMap.size() > topN.getValue()){
                        localTopMap.remove(localTopMap.firstKey());
                    }
                }
                return Collections.singletonList(localTopMap).iterator();
            }
        });
        partitions_topN.saveAsTextFile(args[1]+"/4");

        // STEP-9: find a final top-N
        SortedMap<Integer, String> finalTopN = new TreeMap<Integer, String>();
        List<SortedMap<Integer, String>> allTopN = partitions_topN.collect();
        for (SortedMap<Integer, String> localTopN : allTopN) {
            for (Map.Entry<Integer, String> entry : localTopN.entrySet()) {
                // count = entry.getKey()
                // url = entry.getValue()
                finalTopN.put(entry.getKey(), entry.getValue());
                // keep only top N
                if (finalTopN.size() > N) {
                    finalTopN.remove(finalTopN.firstKey());
                }
            }
        }

        // STEP-10: emit final top-N
        for (Map.Entry<Integer, String> entry : finalTopN.entrySet()) {
            System.out.println(entry.getKey() + "--" + entry.getValue());
        }
        System.exit(0);
    }
}
