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
 * This Reducer does the cleaning process. It concatenates the BiGram with the UniGram (The first word of BiGram).
 * StringPair -> Custom key class responsible for Composite Key
 * Input Format ---> Key: [{Values}] => StringPair: [{Text}]
 * Eg: StringPair{first=replied, second=replied take}: [{replied 196},{replied take 1}]
 * Explanation of input
 *  1.  StringPair consists of 2 texts which creates a composite key
 *  2.  the first part of StringPair is UniGram
 *  3.  the second part of StringPair is BiGram, if present else, UniGram
 *  4.  the value is the BiGrams/UniGrams starting with that UniGram.
 * Output Format ---> (Key,Value) => (StringPair,Text)
 * Eg: (StringPair{first=replied, second=replied take},replied take 1	replied 196)
 * Explanation of output
 *  1. replied take 1 is BiGram and its count
 *  2. replied 196 is UniGram and its count
 */
public class TransformUniBiGramReducer extends Reducer<StringPair,Text,StringPair,Text> {

    private final Logger logger = LoggerFactory.getLogger(TransformUniBiGramReducer.class);

    @Override
    public void reduce(StringPair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //get the UniGram
        String uniGram = key.getFirst().toString();
        String uniGramCount = null;
        for(Text value:values){
            //Split value to check if the incoming value is UniGram/BiGram
            String[] valueArray = value.toString().split("\t");
            if(valueArray[0].split(" ").length == 1){

                uniGramCount = valueArray[0];
            }else{

                String finalValue = valueArray[0] + " "+valueArray[1] + "\t"+uniGram+" "+uniGramCount;
                context.write(key,new Text(finalValue));
            }
        }



    }
}
