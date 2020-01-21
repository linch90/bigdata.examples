package com.linch.bigdata.mapreduce.dataclean;

import com.linch.bigdata.mapreduce.wordcount.WordCount;
import com.linch.bigdata.mapreduce.wordcount.WordCountMapper;
import com.linch.bigdata.mapreduce.wordcount.WordCountReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author linch90
 *
 * A simple dataclean program base on sogou search records.
 * Field1: datetime
 * Field2: userid
 * Field3: search keyword
 * Field4: return order, the record's return order on current page.
 * Field5: click order, the user's click order on current page.
 * Field6: click url
 */
public class DataClean {
    public static void main(String[] args) {
        if(args.length != 2) {
            System.out.println("Usage: DataClean <Src> <Output>");
            System.exit(1);
        }

        String src = args[0];
        String dest = args[1];

        Configuration conf = new Configuration();
        try {
            Job job = Job.getInstance(conf, DataClean.class.getSimpleName());
            job.setJarByClass(DataClean.class);

            FileInputFormat.addInputPath(job, new Path(src));
            FileOutputFormat.setOutputPath(job, new Path(dest));

            job.setMapperClass(DataCleanMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(NullWritable.class);

            job.setNumReduceTasks(0);

            int jobStatusCode = job.waitForCompletion(true) ? 0 : -1;
            System.exit(jobStatusCode);
        } catch (IOException | InterruptedException | ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }
}
