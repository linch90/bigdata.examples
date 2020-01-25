package com.linch.bigdata.mapreduce.topn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * @author linch90
 */
public class TopN extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        if(args.length < 2) {
            System.out.println("Usage: TopN <Src>... <Output>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        System.exit(ToolRunner.run(conf, new TopN(), args));
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = super.getConf();

        Job job = Job.getInstance(conf, TopN.class.getSimpleName());
        job.setJarByClass(TopN.class);

        for(int i = 0; i < args.length - 1; i++){
            FileInputFormat.addInputPath(job, new Path(args[i]));
        }
        String dest = args[args.length - 1];
        FileOutputFormat.setOutputPath(job, new Path(dest));

        job.setMapperClass(TopNMapper.class);
        job.setMapOutputKeyClass(TaobaoOrder.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setPartitionerClass(TaobaoOrderPartitioner.class);
        job.setGroupingComparatorClass(TaobaoOrderGrouping.class);

        job.setReducerClass(TopNReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        return job.waitForCompletion(true) ? 0 : -1;
    }
}
