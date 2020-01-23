# Study examples of big data
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

7. [TopN](mapreduce/src/main/java/com/linch/bigdata/mapreduce/topn/TopN.java)
    1. [TaobaoOrder](mapreduce/src/main/java/com/linch/bigdata/mapreduce/topn/TaobaoOrder.java)
    2. [TopNMapper](mapreduce/src/main/java/com/linch/bigdata/mapreduce/topn/TopNMapper.java)
    3. [TaobaoOrderPartitioner](mapreduce/src/main/java/com/linch/bigdata/mapreduce/topn/TaobaoOrderPartitioner.java)
    4. [TaobaoOrderGrouping](mapreduce/src/main/java/com/linch/bigdata/mapreduce/topn/TaobaoOrderGrouping.java)
    5. [TopNReducer](mapreduce/src/main/java/com/linch/bigdata/mapreduce/topn/TopNReducer.java)
    ```text
    24764639956	2014-12-01 02:20:42.000	原宿风暴显色美瞳彩色隐形艺术眼镜1片 拍2包邮	33.6	2	18067785675
    24377918580	2014-12-17 08:10:25.000	大直径混血美瞳年抛彩色近视隐形眼镜2片包邮	19.8	2	173590154456
    24764639956	2014-11-12 21:28:42.000	之城混血小大直径彩色隐形眼镜1片装 包邮	49.8	2	18115243270
    24764639956	2014-11-22 13:24:46.000	纯铜艾灸盒 温灸器 5年陈艾艾柱 包邮	88	1	38644098439
    24856049592	2014-11-23 01:56:53.000	cosplay艺术片火影忍者美瞳彩色隐形眼镜	65	1	39814158438
    ```
    > Calculate the top n amount consumption of every user and every month base on the taobao order history.
    > Every order contains 6 fields: 1. user id 2. datetime 3. title    4. unit price   5. purchase number  6. product id
    
    > The test data can be found on [https://pan.baidu.com/s/1W7fvYdCRVu-pef_SzV_2mQ](https://pan.baidu.com/s/1W7fvYdCRVu-pef_SzV_2mQ) 提取码：i98h
    
    

