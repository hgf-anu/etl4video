package com.yaxin.etl;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ETLMapper extends Mapper<LongWritable,Text,Text,NullWritable> {

    private Text k = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        String result = handleLine(line);

        //判断返回的行是否为空
        if(result==null){
            context.getCounter("ETL","False").increment(1);
        }else{
            context.getCounter("ETL","True").increment(1);
            k.set(result);
            context.write(k,NullWritable.get());
        }
    }

    /**
     * ETL方法,处理掉长度不够的数据,并且把数据形式做转换->数组之间的分隔符统一
     * @param line 每一行
     * @return
     */
    private String handleLine(String line) {
        return null;
    }
}
