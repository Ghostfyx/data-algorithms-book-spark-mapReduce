package mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @description: MapReduce 实现二次排序启动类
 * @author: fanyeuxiang
 * @createDate: 2020-01-08
 */
public class SecondarySortDriver extends Configured implements Tool {

    private static Logger theLogger = LoggerFactory.getLogger(SecondarySortDriver.class);

    @Override
    public int run(String[] args) throws Exception {
        Configuration configuration = getConf();
        Job job = new Job(configuration);
        job.setJarByClass(SecondarySortDriver.class);
        job.setJobName("SecondarySortDriver");
        // args[0] = input directory
        // args[1] = output directory
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
//        Mapper<K1, V1, K2, V2> --> Reducer<K2, V2, K3, V3>
//        setOutputKeyClass 设置K2,V2
        job.setOutputKeyClass(DateTemperaturePair.class);
//        setOutputValueClass 设置K3,V3
        job.setOutputValueClass(Text.class);
        job.setMapperClass(SecondarySortMapper.class);
        job.setReducerClass(SecondarySortReducer.class);
        job.setPartitionerClass(DateTemperaturePartitioner.class);
        job.setGroupingComparatorClass(DateTemperatureGroupingComparator.class);
        boolean status = job.waitForCompletion(true);
        theLogger.info("run(): status="+status);
        return status ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        // Make sure there are exactly 2 parameters
        if (args.length != 2) {
            theLogger.warn("SecondarySortDriver <input-dir> <output-dir>");
            throw new IllegalArgumentException("SecondarySortDriver <input-dir> <output-dir>");
        }
        int returnStatus = ToolRunner.run(new SecondarySortDriver(), args);
        theLogger.info("returnStatus="+returnStatus);

        System.exit(returnStatus);
    }
}
