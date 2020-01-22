package com.linch.bigdata.mapreduce.commentsplit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;

/**
 * @author linch90
 */
public class RatingSplitOutputFormat extends FileOutputFormat<Text, NullWritable> {
    @Override
    public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();

        boolean isCompressed = getCompressOutput(context);
        CompressionCodec codec = null;
        String extension = "";
        if(isCompressed){
            Class<? extends CompressionCodec> codecClass = getOutputCompressorClass(context, DefaultCodec.class);
            codec = ReflectionUtils.newInstance(codecClass, conf);
            extension = codec.getDefaultExtension();
        }

        Path file = getDefaultWorkFile(context, extension);
        Path parent = file.getParent();
        String fileName = file.getName();
        Path positiveFile = mergePaths(parent, new Path("positive-" + fileName));
        Path negativeFile = mergePaths(parent, new Path("negative-" + fileName));
        FileSystem fs = file.getFileSystem(conf);
        FSDataOutputStream positiveOutStream = fs.create(positiveFile);
        FSDataOutputStream negativeOutStream = fs.create(negativeFile);

        if(isCompressed){
            return new RatingSplitRecordWriter(codec.createOutputStream(positiveOutStream), codec.createOutputStream(negativeOutStream));
        } else {
            return new RatingSplitRecordWriter(positiveOutStream, negativeOutStream);
        }
    }

    private Path mergePaths(Path path1, Path... paths){
        for(Path path : paths) {
            String path1Str = path1.toString();
            if(!path1Str.endsWith("/")){
                path1 = new Path(path1Str + "/" + path);
            } else{
                path1 = new Path(path1Str + path);
            }
        }
        return path1;
    }
}
