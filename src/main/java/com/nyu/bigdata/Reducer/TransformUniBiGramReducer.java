package com.nyu.bigdata.Reducer;

import com.nyu.bigdata.model.StringPair;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import java.util.HashMap;
import java.util.Map;

/**
 * This Reducer is responsible to find Total Uni-Gram and Bi-Gram count.
 * It also calculates Uni-Gram and Bi-Gram count
 * Input Format ---> Key: [{Values}] => Text: [{LongWritable}]
 * Eg: zulu:[{1,1,1,1}]
 * Output Format ---> (Key,Value) => (Text,LongWritable)
 * Eg: zulus, 4
 */
public class TransformUniBiGramReducer extends Reducer<StringPair,Text,StringPair,Text> {

    private final Logger logger = LoggerFactory.getLogger(TransformUniBiGramReducer.class);

    @Override
    public void reduce(StringPair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        logger.info("The key is:{}",key);
        String uniGram = key.getFirst().toString();
        String uniGramCount = null;
        for(Text value:values){
            String[] valueArray = value.toString().split("\t");
            if(valueArray[0].split(" ").length == 1){

                uniGramCount = valueArray[0];
            }else{

                String finalValue = valueArray[0] + " "+valueArray[1] + "\t"+uniGram+" "+uniGramCount;
                context.write(key,new Text(finalValue));
            }
        }
       /* Map<String,String> biGramMapCache = new HashMap<>();
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
        }*/


    }
}
