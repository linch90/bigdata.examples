package com.linch.bigdata.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Lz4Codec;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;

/**
 * @author linch90
 */
public class WriteFileToHdfs {
    public static void main(String[] args) {
        if(args.length != 2) {
            System.out.println("Usage: WriteFileToHdfs <LocalSrc> <HdfsDest>");
            System.exit(1);
        }

        String src = args[0];
        String dest = args[1];

        Configuration conf = new Configuration();

        // Add Lz4Codec compression
        Lz4Codec codec = new Lz4Codec();
        codec.setConf(conf);

        try {
            FileSystem fs = FileSystem.get(URI.create(dest), conf);
            FSDataOutputStream out = fs.create(new Path(dest));

            // Compress file with Lz4Codec before write to HDFS
            CompressionOutputStream compressedOut = codec.createOutputStream(out);

            BufferedInputStream in = new BufferedInputStream(new FileInputStream(src));
            IOUtils.copyBytes(in, compressedOut, 4096, true);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
