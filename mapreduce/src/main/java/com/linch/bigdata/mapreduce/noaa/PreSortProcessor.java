package com.linch.bigdata.mapreduce.noaa;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.Lz4Codec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author linch90
 */
public class PreSortProcessor extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        if(args.length < 2) {
            System.out.println("Usage: PreSortProcessor <NOAA Src>... <Output>");
            System.exit(1);
        }
        Configuration conf = new Configuration();

        System.exit(ToolRunner.run(conf, new PreSortProcessor(), args));
    }

    @Override
    public int run(String[] args) throws Exception {
        String dest = args[args.length - 1];

        Configuration conf = super.getConf();

        Job job = Job.getInstance(conf, super.getClass().getSimpleName());
        job.setJarByClass(PreSortProcessor.class);

        for(int i = 0; i < args.length - 1; i++){
            FileInputFormat.addInputPath(job, new Path(args[i]));
        }

        SequenceFileOutputFormat.setOutputPath(job, new Path(dest));

        SequenceFileOutputFormat.setCompressOutput(job, true);
        SequenceFileOutputFormat.setOutputCompressorClass(job, Lz4Codec.class);
        SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.RECORD);

//        conf.set("mapreduce.output.fileoutputformat.compress", "true");
//        conf.set("mapreduce.output.fileoutputformat.compress.type", "RECORD");
//        conf.set("mapreduce.output.fileoutputformat.compress.codec", "org.apache.hadoop.io.compress.Lz4Codec");

        job.setMapperClass(PreSortProcessorMapper.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setNumReduceTasks(0);

        return job.waitForCompletion(true) ? 0 : -1;
    }
}
