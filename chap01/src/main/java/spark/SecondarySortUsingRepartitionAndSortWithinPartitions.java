package spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

/**
 * @description:
 * Spark/Scala solution to secondary sort using repartitionAndSortWithinPartitions()
 *
 * Repartition the RDD according to the given partitioner and,
 * within each resulting partition, sort records by their keys.
 * This is more efficient than calling repartition and then sorting
 * within each partition because it can push the sorting down into
 * the shuffle machinery.
 *
 * Partitioner: an object that defines how the elements in a key-value
 * pair RDD are partitioned by key. Maps each key to a partition ID,
 * from 0 to numPartitions - 1.
 *
 * @author: fanyeuxiang
 * @createDate: 2020-01-09 14:17
 */
public class SecondarySortUsingRepartitionAndSortWithinPartitions {

    public static void main(String[] args){
        if (args.length != 3) {
            System.err.println("Usage <number-of-partitions> <input-dir> <output-dir>");
            System.exit(1);
        }
        int partitions = Integer.parseInt(args[0]);
        String inputPath = args[1];
        String outputPath = args[2];
        SparkConf conf = new SparkConf();
        conf.setAppName("SecondarySortUsingRepartitionAndSortWithinPartitions");
        final JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> input = sc.textFile(inputPath);
        JavaPairRDD<Tuple2<String, Integer>, Integer> valueToKey = input.mapToPair(new PairFunction<String, Tuple2<String, Integer>, Integer>() {
            @Override
            public Tuple2<Tuple2<String, Integer>, Integer> call(String s) throws Exception {
                String[] tokens = s.split(",");
                Integer value = Integer.parseInt(tokens[3]);
                Tuple2<String, Integer> key  = new Tuple2<>(tokens[0] + "-" + tokens[1], value);
                return new Tuple2<>(key, value);
            }
        });
        JavaPairRDD<Tuple2<String, Integer>, Integer> sorted = valueToKey.repartitionAndSortWithinPartitions(new CustomPartitioner(partitions), TupleComparatorDescending.INSTANCE);

        JavaPairRDD<String, Integer> result = sorted.mapToPair(new PairFunction<Tuple2<Tuple2<String, Integer>, Integer>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<Tuple2<String, Integer>, Integer> tuple2IntegerTuple2) throws Exception {
                return new Tuple2<>(tuple2IntegerTuple2._1._1, tuple2IntegerTuple2._2);
            }
        });
        //
        result.saveAsTextFile(outputPath);

        sc.close();
    }

}
