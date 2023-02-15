package com.nyu.bigdata.Mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AggregateUniGramMapper extends Mapper<LongWritable,Text,Text,Text> {


    @Override
    public void map(LongWritable inputKey,Text inputValue,Context context) throws IOException, InterruptedException {
        String[] value = inputValue.toString().split("\t");
        String biGramDetails = value[1];
        String uniGramDetails = value[2];
        String uniGramWord  = value[2].split(" ")[0];

        context.write(new Text(uniGramWord),new Text(value[1] + "\t"+value[2]));


    }

}
