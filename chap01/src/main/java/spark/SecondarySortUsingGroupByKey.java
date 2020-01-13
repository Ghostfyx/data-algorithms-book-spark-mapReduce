package spark;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import util.SparkUtil;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @description: Spark 使用GroupByKey 方式在内存中进行二次排序（实现方式1）
 * @author: fanyeuxiang
 * @createDate: 2020-01-08 17:17
 */
public class SecondarySortUsingGroupByKey {

    public static void main(String[] args) throws Exception{
        // STEP-1: read input parameters and validate them
        if (args.length < 2) {
            System.err.println("Usage: SecondarySortUsingGroupByKey <input> <output>");
            System.exit(1);
        }
        String inputPath = args[0];
        System.out.println("inputPath=" + inputPath);
        String outputPath = args[1];
        System.out.println("outputPath=" + outputPath);
        // STEP-2: Connect to the Spark master by creating JavaSparkContext object
        final JavaSparkContext ctx = SparkUtil.createJavaSparkContext("SecondarySortUsingGroupByKey");
        // STEP-3: Use ctx to create JavaRDD<String>
        //  input record format: <year><,><month><,><day><,><temperature>
        JavaRDD<String> lines = ctx.textFile(inputPath, 1);
        // STEP-4: create (key, value) pairs from JavaRDD<String> where
        System.out.println("===  DEBUG STEP-4 ===");
        /**
         * key is the {name} and value is a pair of (time, value).
         * The resulting RDD will be JavaPairRDD<String, Tuple2<Integer, Integer>>.
         * convert each record into Tuple2(name, time, value)
         * PairFunction<T, K, V>	T => Tuple2(K, V) where K=String and V=Tuple2<Integer, Integer>
         *                                                                                          input   K       V
         **/
        JavaPairRDD<String, Tuple2<Integer, Integer>> pairs = lines.mapToPair(new PairFunction<String, String, Tuple2<Integer, Integer>>() {
            @Override
            public Tuple2<String, Tuple2<Integer, Integer>> call(String s) throws Exception {
                String[] tokens = s.split(","); // x,2,5
                Tuple2<Integer, Integer> timevalue = new Tuple2<>(Integer.parseInt(tokens[1]), Integer.parseInt(tokens[2]));
                return new Tuple2<String, Tuple2<Integer, Integer>>(tokens[0], timevalue);
            }
        });
        // STEP-5: validate STEP-4, we collect all values from JavaPairRDD<> and print it. 注意此步骤只是用于调试，不建议使用collect方法！！！
        List<Tuple2<String, Tuple2<Integer, Integer>>> output = pairs.collect();
        for (Tuple2 t : output){
            Tuple2<Integer, Integer> timevalue = (Tuple2<Integer, Integer>) t._2;
            System.out.println(t._1 + "," + timevalue._1 + "," + timevalue._2);
        }
        // STEP-6: We group JavaPairRDD<> elements by the key ({name}).
        JavaPairRDD<String, Iterable<Tuple2<Integer, Integer>>> groups = pairs.groupByKey();
        // STEP-7: validate STEP-6, we collect all values from JavaPairRDD<> and print it.
        System.out.println("===  DEBUG STEP-6 ===");
        List<Tuple2<String, Iterable<Tuple2<Integer, Integer>>>> output2 = groups.collect();
        for (Tuple2<String, Iterable<Tuple2<Integer, Integer>>> t : output2) {
            Iterable<Tuple2<Integer, Integer>> list = t._2;
            System.out.println(t._1);
            for (Tuple2<Integer, Integer> t2 : list) {
                System.out.println(t2._1 + "," + t2._2);
            }
            System.out.println("=====");
        }
        //STEP-8: Sort the reducer's values in RAM and this will give us the final output.
        JavaPairRDD<String, Iterable<Tuple2<Integer, Integer>>> sorted = groups.mapValues(new Function<Iterable<Tuple2<Integer, Integer>>,      // input
                        Iterable<Tuple2<Integer, Integer>>       // output
                        >() {
            @Override
            public Iterable<Tuple2<Integer, Integer>> call(Iterable<Tuple2<Integer, Integer>> s) {
                List<Tuple2<Integer, Integer>> sortedList = new ArrayList<>(iterableToList(s));
                // 内存中对Reducer的值排序
                Collections.sort(sortedList, SparkTupleComparator.INSTANCE);
                return sortedList;
            }
        });

        // STEP-9: validate STEP-8, we collect all values from JavaPairRDD<> and print it.
        System.out.println("===  DEBUG STEP-8 ===");
        List<Tuple2<String, Iterable<Tuple2<Integer, Integer>>>> output3 = sorted.collect();
        for (Tuple2<String, Iterable<Tuple2<Integer, Integer>>> t : output3) {
            Iterable<Tuple2<Integer, Integer>> list = t._2;
            System.out.println(t._1);
            for (Tuple2<Integer, Integer> t2 : list) {
                System.out.println(t2._1 + "," + t2._2);
            }
            System.out.println("=====");
        }

        sorted.saveAsTextFile(outputPath);

        System.exit(0);
    }

    static List<Tuple2<Integer,Integer>> iterableToList(Iterable<Tuple2<Integer,Integer>> iterable) {
        List<Tuple2<Integer,Integer>> list = new ArrayList<Tuple2<Integer,Integer>>();
        for (Tuple2<Integer,Integer> item : iterable) {
            list.add(item);
        }
        return list;
    }

}
