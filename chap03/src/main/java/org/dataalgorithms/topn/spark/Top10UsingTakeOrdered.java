package org.dataalgorithms.topn.spark;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.List;

/**
 * @description:
 * Assumption: for all input (K, V), K's are non-unique.
 * This class implements Top-N design pattern for N > 0.
 * The main assumption is that for all input (K, V)'s, K's
 * are non-unique. It means that you will find entries like
 * (A, 2), ..., (A, 5),...
 *
 * This is a general top-N algorithm which will work unique
 * and non-unique keys.
 *
 * This class may be used to find bottom-N as well (by
 * just keeping N-smallest elements in the set.
 *
 *  Top-10 Design Pattern: “Top Ten” Structure
 *
 *  1. map(input) => (K, V)
 *
 *  2. reduce(K, List<V1, V2, ..., Vn>) => (K, V),
 *                where V = V1+V2+...+Vn
 *     now all K's are unique
 *
 *  3. Find Top-N using the following high-level Spark API:
 *     java.util.List<T> takeOrdered(int N, java.util.Comparator<T> comp)
 *     Returns the first N elements from this RDD as defined by the specified
 *     Comparator[T] and maintains the order.
 * @author: fanyeuxiang
 * @createDate: 2020-01-13
 */
public class Top10UsingTakeOrdered {

    public static void main(String[] args){
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
        // STEP-7: find final top-N by calling takeOrdered()z
        List<Tuple2<String, Integer>> topNResult = uniqueKeys.takeOrdered(N, MyTupleComparator.INSTANCE);
        // STEP-8: emit final top-N
        for (Tuple2<String, Integer> entry : topNResult) {
            System.out.println(entry._2 + "--" + entry._1);
        }

        System.exit(0);
    }

}
