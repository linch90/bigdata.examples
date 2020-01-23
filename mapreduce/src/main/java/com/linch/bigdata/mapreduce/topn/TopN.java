package com.linch.bigdata.mapreduce.topn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author linch90
 */
public class TopN {
    public static void main(String[] args) {
        if(args.length < 2) {
            System.out.println("Usage: TopN <Src>... <Output>");
            System.exit(1);
        }

        String dest = args[args.length - 1];

        Configuration conf = new Configuration();

        try {
            Job job = Job.getInstance(conf, TopN.class.getSimpleName());
            job.setJarByClass(TopN.class);

            for(int i = 0; i < args.length - 1; i++){
                FileInputFormat.addInputPath(job, new Path(args[i]));
            }

            FileOutputFormat.setOutputPath(job, new Path(dest));

            job.setMapperClass(TopNMapper.class);
            job.setMapOutputKeyClass(TaobaoOrder.class);
            job.setMapOutputValueClass(DoubleWritable.class);

            job.setPartitionerClass(TaobaoOrderPartitioner.class);
            job.setGroupingComparatorClass(TaobaoOrderGrouping.class);

            job.setReducerClass(TopNReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(DoubleWritable.class);

            int jobStatusCode = job.waitForCompletion(true) ? 0 : -1;
            System.exit(jobStatusCode);
        } catch (IOException | InterruptedException | ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }
}
