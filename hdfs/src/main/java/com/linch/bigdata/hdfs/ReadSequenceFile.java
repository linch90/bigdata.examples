package com.linch.bigdata.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;

/**
 * @author linch90
 */
public class ReadSequenceFile {
    public static void main(String[] args) {
        String src = "hdfs://node01:8020/temp/helloSeqLz4";

        Configuration conf = new Configuration();
        SequenceFile.Reader.Option pathOption = SequenceFile.Reader.file(new Path(src));
        SequenceFile.Reader.Option bufferOption = SequenceFile.Reader.bufferSize(4096);
        SequenceFile.Reader reader = null;
        try {
            reader = new SequenceFile.Reader(conf, pathOption, bufferOption);
            Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
            Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);

            long position = reader.getPosition();

            while(reader.next(key, value)){
                String syncMarker = reader.syncSeen() ? "*" : "";
                System.out.printf("[%s%s]\t%s\t%s\n", position, syncMarker, key, value);
                position = reader.getPosition();
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(reader);
        }
    }
}
