package com.linch.bigdata.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.Lz4Codec;

import java.io.IOException;

/**
 * @author linch90
 */
public class WriteSequenceFile {
    private static final String[] DATA = {
            "Roast seven meatballs, cabbage, and marmalade in a large grinder over medium heat, sauté for five minutes and whisk with some eggs.",
            "Understanding at the planet was the starlight travel of galaxy, infiltrated to a senior particle.",
            "The proud crewmate patiently outweighs the ship.",
            "Ugly, real tribbles wisely acquire a post-apocalyptic, strange starship.",
            "The strange hur'q technically consumes the particle.",
            "Mystery at the wormhole was the understanding of definition, feeded to a united star.",
            "Tribbles view on sensor at subspace!",
            "The post-apocalyptic planet technically teleports the pathway.",
            "Adventure at the habitat unearthlysurprisingly, indeed that is when greatly exaggerated astronauts view.",
            "Flight at the infinity room was the sonic shower of history, questioned to a distant crew."
    };

    public static void main(String[] args) {
        // if /temp not exist -> CLI: hdfs dfs -mkdir /temp
        String dest = "hdfs://node01:8020/temp/helloSeqLz4";
        Configuration conf = new Configuration();
        try {
            SequenceFile.Writer.Option pathOption = SequenceFile.Writer.file(new Path(dest));
            SequenceFile.Writer.Option keyOption = SequenceFile.Writer.keyClass(IntWritable.class);
            SequenceFile.Writer.Option valueOption = SequenceFile.Writer.valueClass(Text.class);

            // 压缩级别： Record
//            SequenceFile.Writer.Option compressionOption = SequenceFile.Writer.compression(SequenceFile.CompressionType.RECORD);

            // 压缩级别：Block，不指定压缩算法
//            SequenceFile.Writer.Option compressionOption = SequenceFile.Writer.compression(SequenceFile.CompressionType.BLOCK);

            // 压缩级别：Block，Lz4Codec压缩算法
            Lz4Codec codec = new Lz4Codec();

            codec.setConf(conf);

            SequenceFile.Writer.Option compressionOption = SequenceFile.Writer.compression(SequenceFile.CompressionType.BLOCK, codec);

            SequenceFile.Writer writer = SequenceFile.createWriter(conf, pathOption, keyOption, valueOption, compressionOption);

            IntWritable key = new IntWritable();
            Text value = new Text();

            for (int i = 0; i < 100000; i++){
                key.set(i);
                value.set(DATA[i % DATA.length]);
                writer.append(key, value);
            }
            IOUtils.closeStream(writer);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
