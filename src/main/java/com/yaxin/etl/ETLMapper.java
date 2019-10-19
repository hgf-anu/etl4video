package com.yaxin.etl;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ETLMapper extends Mapper<LongWritable,Text,Text,NullWritable> {

    private Text k = new Text();
    //注意:只要有字符串的拼接一定要使用Stringbuilder
    private StringBuilder sb = new StringBuilder();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        String result = handleLine(line);

        //判断返回的行是否为空,为空的数据被清洗掉
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
     * @return 处理后的行
     */
    private String handleLine(String line) {
        //初始化StringBuilder
        sb.delete(0,sb.length());
        String[] fields = line.split("\t");
        //如何字段个数小于9的行是数据丢失的行,应该舍弃
        if(fields.length<9){
            return null;
        }
        //下标从0开始,第4个元素进行etl,也就是fields[3]
        fields[3] = fields[3].replace(" ", "");

        for (int i = 0; i < fields.length; i++) {
            //如果是最后一个,就不用使用\t拼接了
            if(i==fields.length-1){
                sb.append(fields[i]);
            }else if(i<9){
                //如果是前九个,需要用\t拼接
                sb.append(fields[i]).append("\t");
            }else{
                // 如果都不是前2种情况,说明就是有推荐相同电影的值了,使用&拼接起来
                // 同时这里也巧妙的避开了只有一个推荐电影的情况,因为遍历的时候如果推荐电影字段的值只有一个,
                // 又为最后一个元素的时候,就会进入if语句的第一个判断中
                sb.append(fields[i]).append("&");
            }
        }
        return sb.toString();
    }
}
