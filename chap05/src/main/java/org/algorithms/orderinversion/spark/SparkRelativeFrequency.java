package org.algorithms.orderinversion.spark;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.*;

/**
 * @description:
 * @author: fanyeuxiang
 * @createDate: 2020-01-14
 */
public class SparkRelativeFrequency {

    public static void main(String[] args) {
        if (args.length != 3) {
            System.out.println("usage:<input> <output> <window>");
            System.exit(-1);
        }
        JavaSparkContext sc = new JavaSparkContext();
        String inputPath = args[0];
        String outputPath = args[1];
        Integer window = Integer.valueOf(args[2]);
        final Broadcast<Integer> neighborWindow = sc.broadcast(window);

        JavaRDD<String> rawData = sc.textFile(inputPath);
        // flatMapToPair : 创建键值对RDD, 一个key返回多个value
        JavaPairRDD<String, Tuple2<String, Integer>> pairsRDD = rawData.flatMapToPair(new PairFlatMapFunction<String, String, Tuple2<String, Integer>>() {
            @Override
            public Iterator<Tuple2<String, Tuple2<String, Integer>>> call(String s) throws Exception {
                String[] tokens = s.split("\\s");
                if (null == tokens || tokens.length < 2) {
                    return null;
                }
                List<Tuple2<String, Tuple2<String, Integer>>> list = new ArrayList<Tuple2<String, Tuple2<String, Integer>>>();
                for (int i = 0; i < tokens.length; i++) {
                    int start = (i - neighborWindow.value() < 0) ? 0 : i - neighborWindow.value();
                    int end = (i + neighborWindow.value() > tokens.length - 1) ? tokens.length - 1 : i + neighborWindow.value();
                    for (int j = start; j <= end; j++) {
                        if (i == j) {
                            continue;
                        } else {
                            Tuple2<String, Integer> neighbor = new Tuple2<String, Integer>(tokens[j], 1);
                            list.add(new Tuple2<>(tokens[i], neighbor));
                        }
                    }
                }
                return list.iterator();
            }
        });

        // (word, sum(word))
        JavaPairRDD<String, Integer> totalByKey = pairsRDD.mapToPair((Tuple2<String, Tuple2<String, Integer>> s) -> {
            return new Tuple2<>(s._1, s._2._2);
        }).reduceByKey((Integer v1, Integer v2) -> {
            return v1 + v2;
        });

        JavaPairRDD<String, Iterable<Tuple2<String, Integer>>> grouped = pairsRDD.groupByKey();

        // to (word, (neighbour, sum(neighbour)))
        JavaPairRDD<String, Tuple2<String, Integer>> uniquePairs = grouped.flatMapValues(new Function<Iterable<Tuple2<String, Integer>>, Iterable<Tuple2<String, Integer>>>() {
            @Override
            public Iterable<Tuple2<String, Integer>> call(Iterable<Tuple2<String, Integer>> v1) throws Exception {
                Map<String, Integer> map = new HashMap<>();
                List<Tuple2<String, Integer>> list = new ArrayList<>();
                Iterator<Tuple2<String, Integer>> iterator = v1.iterator();
                while (iterator.hasNext()) {
                    Tuple2<String, Integer> value = iterator.next();
                    int total = value._2;
                    if (map.containsKey(value._1)) {
                        total += map.get(value._1);
                    }
                    map.put(value._1, total);
                }
                for (Map.Entry<String, Integer> kv : map.entrySet()) {
                    list.add(new Tuple2<String, Integer>(kv.getKey(), kv.getValue()));
                }
                return list;
            }
        });
        // to (word, ((neighbour, sum(neighbour)), sum(word)))
        JavaPairRDD<String, Tuple2<Tuple2<String, Integer>, Integer>> joined = uniquePairs.join(totalByKey);

        JavaPairRDD<Tuple2<String, String>, Double> relativeFrequency = joined.mapToPair((Tuple2<String, Tuple2<Tuple2<String, Integer>, Integer>> tuple)->{
            return new Tuple2<Tuple2<String, String>, Double>(new Tuple2<String, String>(tuple._1, tuple._2._1._1), ((double) tuple._2._1._2 / tuple._2._2));
        });

        JavaRDD<String> formatResult_tab_separated = relativeFrequency.map((Tuple2<Tuple2<String, String>, Double> tuple) ->{
            return tuple._1._1 + "\t" + tuple._1._2 + "\t" + tuple._2;
        });
        // save output
        formatResult_tab_separated.saveAsTextFile(outputPath);
        // done
        sc.close();
    }

}
