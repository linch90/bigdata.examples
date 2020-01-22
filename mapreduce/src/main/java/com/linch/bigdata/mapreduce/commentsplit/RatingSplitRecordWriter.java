package com.linch.bigdata.mapreduce.commentsplit;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.io.OutputStream;

/**
 * @author linch90
 */
public class RatingSplitRecordWriter extends RecordWriter<Text, NullWritable> {

    private final OutputStream positiveOutStream;
    private final OutputStream negativeOutStream;

    RatingSplitRecordWriter(OutputStream positiveOutStream, OutputStream negativeOutStream){
        this.positiveOutStream = positiveOutStream;
        this.negativeOutStream = negativeOutStream;
    }

    @Override
    public void write(Text text, NullWritable nullWritable) throws IOException, InterruptedException {
        if(text.toString().split("\t")[9].equals("0")){
            positiveOutStream.write(text.toString().getBytes());
            positiveOutStream.write("\r\n".getBytes());
        } else {
            negativeOutStream.write(text.toString().getBytes());
            negativeOutStream.write("\r\n".getBytes());
        }
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        IOUtils.closeStream(this.positiveOutStream);
        IOUtils.closeStream(this.negativeOutStream);
    }
}
