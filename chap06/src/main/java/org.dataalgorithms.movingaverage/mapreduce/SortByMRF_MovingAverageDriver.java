package org.dataalgorithms.movingaverage.mapreduce;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @description:
 * @author: fanyeuxiang
 * @createDate: 2020-01-15
 */
public class SortByMRF_MovingAverageDriver extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int returnStatus = ToolRunner.run(new SortByMRF_MovingAverageDriver(), args);
        System.out.println("returnStatus="+returnStatus);
        System.exit(returnStatus);
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 3){
            System.err.println("Usage: SortByMRF_MovingAverageDriver <input> <output> <window_size> ");
            System.exit(1);
        }
        String inputPath = args[0];
        String outputPath = args[1];
        int windowSize = Integer.parseInt(args[2]);

        Job job = new Job(getConf());
        job.setJobName("SortByMRF_MovingAverage");
        job.setJarByClass(SortByMRF_MovingAverageDriver.class);

        FileInputFormat.setInputPaths(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapperClass(SortByMRF_MovingAverageMapper.class);
        job.setMapOutputKeyClass(CompositeKey.class);
        job.setMapOutputValueClass(TimeSeriesData.class);
        job.setPartitionerClass(CompositeKeyPartitioner.class);
        job.setGroupingComparatorClass(CompositeKeyGroupingComparator.class);
        job.setReducerClass(SortByMRF_MovingAverageReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        boolean status = job.waitForCompletion(true);
        System.out.println("run(): status="+status);
        return status ? 0 : 1;
    }
}
