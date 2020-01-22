package com.linch.bigdata.mapreduce.commentsplit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.Lz4Codec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * @author linch90
 */
public class CommentSplit {
    public static void main(String[] args) {
        if(args.length < 2) {
            System.out.println("Usage: CommentSplit <Src>... <Output>");
            System.exit(1);
        }

        String dest = args[args.length - 1];

        Configuration conf = new Configuration();
        try {
            Job job = Job.getInstance(conf, "Split comments by rating");
            job.setJarByClass(CommentSplit.class);

            for(int i = 0; i < args.length - 1; i++){
                FileInputFormat.setInputPaths(job, new Path(args[i]));
            }

            job.setOutputFormatClass(RatingSplitOutputFormat.class);
            RatingSplitOutputFormat.setOutputPath(job, new Path(dest));

//            RatingSplitOutputFormat.setCompressOutput(job, true);
//            RatingSplitOutputFormat.setOutputCompressorClass(job, Lz4Codec.class);

            job.setMapperClass(CommentSplitMapper.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(NullWritable.class);

            try {
                job.waitForCompletion(true);
            } catch (InterruptedException | ClassNotFoundException e) {
                e.printStackTrace();
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
