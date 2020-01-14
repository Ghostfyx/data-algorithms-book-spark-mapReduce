package org.dataalgorithms.leftjoin.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * @description:
 * @author: fanyeuxiang
 * @createDate: 2020-01-13
 */
public class LocationCountDriver {

    public static void main(String[] args) throws Exception {
        Path input = new Path(args[0]);
        Path output = new Path(args[1]);
        Configuration conf = new Configuration();

        Job job = new Job(conf);
        job.setJarByClass(LocationCountDriver.class);
        job.setJobName("left_join_step_2_LocationCountDriver");

        FileInputFormat.addInputPath(job, input);
        // 读取后按照（key,value）对表示一条记录；
        //一行中可能被分成多个区域（可能是制表符、逗号或者其他作为分隔符），第一个区域作为key，其他区域作为value。
        job.setInputFormatClass(KeyValueTextInputFormat.class);

        job.setMapperClass(LocationCountMapper.class);
        job.setReducerClass(LocationCountReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileOutputFormat.setOutputPath(job, output);
        job.setOutputFormatClass(TextOutputFormat.class);
        if (job.waitForCompletion(true)) {
            return;
        }
        else {
            throw new Exception("LocationCountDriver Failed");
        }
    }
}
