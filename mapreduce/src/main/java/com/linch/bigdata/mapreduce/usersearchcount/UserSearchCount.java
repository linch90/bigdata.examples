package com.linch.bigdata.mapreduce.usersearchcount;

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
 * A simple user search count program base on sogou search records that has been data cleaned.
 */
public class UserSearchCount {
    public static void main(String[] args) {
        if(args.length < 2) {
            System.out.println("Usage: UserSearchCount <Src>... <Output>");
            System.exit(1);
        }

        String dest = args[args.length - 1];

        Configuration conf = new Configuration();
        try {
            Job job = Job.getInstance(conf, UserSearchCount.class.getSimpleName());
            job.setJarByClass(UserSearchCount.class);

            for(int i = 0; i < args.length - 1; i++){
                FileInputFormat.addInputPath(job, new Path(args[i]));
            }
            FileOutputFormat.setOutputPath(job, new Path(dest));

            job.setMapperClass(UserSearchCountMapper.class);
            job.setCombinerClass(UserSearchCountReducer.class);
            job.setReducerClass(UserSearchCountReducer.class);
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
