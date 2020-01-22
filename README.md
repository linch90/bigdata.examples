# 大数据学习示例
## HDFS
1. [WriteFileToHdfs](hdfs/src/main/java/com/linch/bigdata/hdfs/WriteFileToHdfs.java)
2. [ReadFileFromHdfs](hdfs/src/main/java/com/linch/bigdata/hdfs/ReadFileFromHdfs.java)
3. [WriteSequenceFile](hdfs/src/main/java/com/linch/bigdata/hdfs/WriteSequenceFile.java)
4. [ReadSequenceFile](hdfs/src/main/java/com/linch/bigdata/hdfs/ReadSequenceFile.java)

## MapReduce
1. [WordCount](mapreduce/src/main/java/com/linch/bigdata/mapreduce/wordcount/WordCount.java)
    1. [WordCountMapper](mapreduce/src/main/java/com/linch/bigdata/mapreduce/wordcount/WordCountMapper.java)
    2. [WordCountReducer](mapreduce/src/main/java/com/linch/bigdata/mapreduce/wordcount/WordCountReducer.java)

2. [DataClean](mapreduce/src/main/java/com/linch/bigdata/mapreduce/dataclean/DataClean.java)
    1. [DataCleanMapper](mapreduce/src/main/java/com/linch/bigdata/mapreduce/dataclean/DataCleanMapper.java)
    > A simple data clean program base on sogou search records.
    > Every record(line) contains 6 fields:
    > 1. datetime
    > 2. userid
    > 3. search keyword
    > 4. return order, the record's return order on current page.
    > 5. click order, the user's click order on current page.
    > 6. click url
    >
    > The sogou search records can be found on [https://pan.baidu.com/s/1aSvsmIPSRm_ukDQKxruLgQ](https://pan.baidu.com/s/1aSvsmIPSRm_ukDQKxruLgQ) 提取码：3ype

3. [UserSearchCount](mapreduce/src/main/java/com/linch/bigdata/mapreduce/usersearchcount/UserSearchCount.java)
    1. [UserSearchCountMapper](mapreduce/src/main/java/com/linch/bigdata/mapreduce/usersearchcount/UserSearchCountMapper.java)
    2. [UserSearchCountReducer](mapreduce/src/main/java/com/linch/bigdata/mapreduce/usersearchcount/UserSearchCountReducer.java)
    > A simple user search count program base on sogou search records that has been data cleaned, that is the output from [DataClean](mapreduce/src/main/java/com/linch/bigdata/mapreduce/dataclean/DataClean.java).

4. [SequenceFileWriter](mapreduce/src/main/java/com/linch/bigdata/mapreduce/sequencefile/SequenceFileWriter.java)
    1. [SmallFileInputFormat](mapreduce/src/main/java/com/linch/bigdata/mapreduce/sequencefile/SmallFileInputFormat.java)
    2. [SmallFileRecordReader](mapreduce/src/main/java/com/linch/bigdata/mapreduce/sequencefile/SmallFileRecordReader.java)
    3. [SequenceFileMapper](mapreduce/src/main/java/com/linch/bigdata/mapreduce/sequencefile/SequenceFileMapper.java)
    > Combines small files to sequence file. Store as [key: filename, value: content bytes]

5. [CommentSplit](mapreduce/src/main/java/com/linch/bigdata/mapreduce/commentsplit/CommentSplit.java)
    1. [CommentSplitMapper](mapreduce/src/main/java/com/linch/bigdata/mapreduce/commentsplit/CommentSplitMapper.java)
    2. [RatingSplitOutputFormat](mapreduce/src/main/java/com/linch/bigdata/mapreduce/commentsplit/RatingSplitOutputFormat.java)
    3. [RatingSplitRecordWriter](mapreduce/src/main/java/com/linch/bigdata/mapreduce/commentsplit/RatingSplitRecordWriter.java)
    > Split comments by users' rating. The input data can be found on [https://pan.baidu.com/s/1ZC98VXdD-8xxoSbr6e-vlA](https://pan.baidu.com/s/1ZC98VXdD-8xxoSbr6e-vlA) 提取码：x5sg
    > 
    > The 10th field of every record is the rating, which 0 is positive rating.

6. [PersonSortMain](mapreduce/src/main/java/com/linch/bigdata/mapreduce/multipropsort/PersonSortMain.java)
```text
queen	20	12000
pompeya	22	13000
vexento 22	9000
onerepublic	19	12000
aaron	19	10000
damon	30	29000
raney    29	40000
```
> Sort the personInfo(name age salary) by salary desc, then by age asc.