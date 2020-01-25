package com.linch.bigdata.mapreduce.commentsplit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.Lz4Codec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * @author linch90
 */
public class CommentSplit extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        if(args.length < 2) {
            System.out.println("Usage: CommentSplit <Src>... <Output>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        System.exit(ToolRunner.run(conf, new CommentSplit(), args));
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = super.getConf();

        Job job = Job.getInstance(conf, "Split comments by rating");
        job.setJarByClass(CommentSplit.class);

        for(int i = 0; i < args.length - 1; i++){
            FileInputFormat.setInputPaths(job, new Path(args[i]));
        }
        job.setOutputFormatClass(RatingSplitOutputFormat.class);
        String dest = args[args.length - 1];
        RatingSplitOutputFormat.setOutputPath(job, new Path(dest));

//        RatingSplitOutputFormat.setCompressOutput(job, true);
//        RatingSplitOutputFormat.setOutputCompressorClass(job, Lz4Codec.class);

        job.setMapperClass(CommentSplitMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setOutputFormatClass(RatingSplitOutputFormat.class);

        return job.waitForCompletion(true) ? 0 : -1;
    }
}
