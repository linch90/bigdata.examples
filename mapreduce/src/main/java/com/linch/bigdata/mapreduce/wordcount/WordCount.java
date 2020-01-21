package com.linch.bigdata.mapreduce.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author linch90
 */
public class WordCount {
    public static void main(String[] args) {
        if(args.length != 2) {
            System.out.println("Usage: WordCount <Src> <Output>");
            System.exit(1);
        }

        String src = args[0];
        String dest = args[1];

        Configuration conf = new Configuration();
        try {
            Job job = Job.getInstance(conf, WordCount.class.getSimpleName());
            job.setJarByClass(WordCount.class);

            FileInputFormat.addInputPath(job, new Path(src));
            FileOutputFormat.setOutputPath(job, new Path(dest));

            job.setMapperClass(WordCountMapper.class);
            job.setCombinerClass(WordCountReducer.class);
            job.setReducerClass(WordCountReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

            int jobStatusCode = job.waitForCompletion(true) ? 0 : -1;
            System.exit(jobStatusCode);
        } catch (IOException | InterruptedException | ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }
}
