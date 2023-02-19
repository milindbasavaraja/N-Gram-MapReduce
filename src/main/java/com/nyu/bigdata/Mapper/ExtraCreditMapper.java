package com.nyu.bigdata.Mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ExtraCreditMapper extends Mapper<LongWritable,Text,Text, Text> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] biGramDetails = value.toString().split("\t");
        String biGramKey = biGramDetails[0];


        if(biGramKey.split(" ")[0].equals("states")){
            context.write(new Text("states"),new Text(value));
        }


    }
}
