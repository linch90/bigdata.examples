package com.linch.bigdata.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;

/**
 * @author zlchu
 */
public class ReadFileFromHdfs {
    public static void main(String[] args) {
        if(args.length != 2) {
            System.out.println("Usage: ReadFileFromHdfs <HdfsSrc> <LocalDest>");
            System.exit(1);
        }
        String src = args[0];
        String dest = args[1];
        Configuration conf = new Configuration();
        try{
            FileSystem fs = FileSystem.get(URI.create(src), conf);
            FSDataInputStream in = fs.open(new Path(src));
            BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(dest));
            IOUtils.copyBytes(in, out, 4096, true);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
