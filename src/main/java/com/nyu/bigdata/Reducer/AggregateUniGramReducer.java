package com.nyu.bigdata.Reducer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import java.util.HashMap;
import java.util.Map;


public class AggregateUniGramReducer extends Reducer<Text,Text,Text,Text> {

    private final Logger logger = LoggerFactory.getLogger(AggregateUniGramReducer.class);

    @Override
    public void reduce(Text key,Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Map<String,String> biGramMapCache = new HashMap<>();
        long uniGramWordCount = 0;
        String uniGramWord = key.toString();

        for(Text value:values){
            String[] valueDetails = value.toString().split("\t");
            String uniGramDetails = valueDetails[1];
            String biGramDetails = valueDetails[0];
            String biGramWord  =biGramDetails.split(" ")[0]+" " + biGramDetails.split(" ")[1];
            biGramMapCache.put(biGramWord,biGramDetails);
            long uniGramCount = Long.parseLong(uniGramDetails.split(" ")[1]);

            uniGramWordCount = uniGramWordCount +  uniGramCount;
        }

        String finalUniGramDetail = uniGramWord + " "+uniGramWordCount;

        for(String biGramWord : biGramMapCache.keySet()){
            String finalValue = biGramMapCache.get(biGramWord)+"\t" + finalUniGramDetail;
            context.write(new Text(biGramWord),new Text(finalValue));
        }


    }
}
