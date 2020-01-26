package com.linch.bigdata.mapreduce.noaa;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.Lz4Codec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.net.URI;

/**
 * @author linch90
 */
public class TemperatureSorting extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        if(args.length < 2) {
            System.out.println("Usage: TemperatureSorting <Src>... <Output>");
            System.exit(1);
        }

        Configuration conf = new Configuration();

        System.exit(ToolRunner.run(conf, new TemperatureSorting(), args));
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = super.getConf();

        Job job = Job.getInstance(conf, super.getClass().getSimpleName());
        job.setJarByClass(super.getClass());

        for(int i = 0; i < args.length - 1; i++) {
            FileInputFormat.addInputPath(job, new Path(args[i]));
        }

        FileOutputFormat.setOutputPath(job, new Path(args[args.length - 1]));

        job.setInputFormatClass(SequenceFileInputFormat.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setPartitionerClass(TotalOrderPartitioner.class);

        job.setNumReduceTasks(6);

        InputSampler.Sampler<IntWritable, Text> sampler = new InputSampler.RandomSampler<>(0.1, 1000, 10);
        InputSampler.writePartitionFile(job, sampler);

        String partitionFile = TotalOrderPartitioner.getPartitionFile(job.getConfiguration());

        DistributedCache.addCacheFile(URI.create(partitionFile), job.getConfiguration());

        SequenceFileOutputFormat.setCompressOutput(job, true);
        SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.RECORD);
        SequenceFileOutputFormat.setOutputCompressorClass(job, Lz4Codec.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
