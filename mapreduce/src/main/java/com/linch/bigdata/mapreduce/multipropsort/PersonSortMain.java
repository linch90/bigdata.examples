package com.linch.bigdata.mapreduce.multipropsort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author linch90
 */
public class PersonSortMain {
    public static void main(String[] args) {
        if(args.length < 2) {
            System.out.println("Usage: PersonSortMain <Src>... <Output>");
            System.exit(1);
        }

        String dest = args[args.length - 1];

        Configuration conf = new Configuration();
        try {
            Job job = Job.getInstance(conf, "Sort Person by salary desc, age asc");
            job.setJarByClass(PersonSortMain.class);

            for(int i = 0; i < args.length -1; i++){
                FileInputFormat.addInputPath(job, new Path(args[i]));
            }
            FileOutputFormat.setOutputPath(job, new Path(dest));

            job.setMapperClass(PersonMapper.class);
            job.setReducerClass(PersonReducer.class);

            job.setOutputKeyClass(Person.class);
            job.setOutputValueClass(NullWritable.class);

            int jobStatusCode = job.waitForCompletion(true) ? 0 : -1;
            System.exit(jobStatusCode);
        } catch (IOException | InterruptedException | ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }
}
