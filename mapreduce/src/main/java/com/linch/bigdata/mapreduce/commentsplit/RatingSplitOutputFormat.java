package com.linch.bigdata.mapreduce.commentsplit;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author linch90
 */
public class RatingSplitOutputFormat extends FileOutputFormat<Text, NullWritable> {
    @Override
    public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        FileSystem fs = FileSystem.get(context.getConfiguration());
        Path outputPath = getOutputPath(context);
        Path positivePath = Path.mergePaths(outputPath, new Path("/positive"));
        Path negativePath = Path.mergePaths(outputPath, new Path("/negative"));
        FSDataOutputStream positiveOutStream = fs.create(positivePath);
        FSDataOutputStream negativeOutStream = fs.create(negativePath);
        return new RatingSplitRecordWriter(positiveOutStream, negativeOutStream);
    }
}
