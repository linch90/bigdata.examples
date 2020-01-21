package com.linch.bigdata.mapreduce.sequencefile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

/**
 * @author linch90
 */
public class SequenceFileWriter {
    public static void main(String[] args) {
        if(args.length < 2) {
            System.out.println("Usage: SequenceFileWriter <Src>... <Output>");
            System.exit(1);
        }

        String dest = args[args.length - 1];

        Configuration conf = new Configuration();
        try {
            Job job = Job.getInstance(conf, "Combine small files to sequenceFile");
            job.setJarByClass(SequenceFileWriter.class);

            job.setInputFormatClass(SmallFileInputFormat.class);
            for(int i = 0; i < args.length - 1; i++){
                SmallFileInputFormat.addInputPath(job, new Path(args[i]));
            }

            job.setOutputFormatClass(SequenceFileOutputFormat.class);
            SequenceFileOutputFormat.setOutputPath(job, new Path(dest));

            job.setMapperClass(SequenceFileMapper.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(BytesWritable.class);

            int jobStatusCode = job.waitForCompletion(true) ? 0 : -1;
            System.exit(jobStatusCode);
        } catch (IOException | InterruptedException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
