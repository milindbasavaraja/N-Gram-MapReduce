package com.nyu.bigdata.Mapper;

import com.nyu.bigdata.model.StringPair;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TransformUniBiGramMapper extends Mapper<LongWritable,Text, StringPair,Text> {



    @Override
    public void map(LongWritable inputKey,Text inputValue,Context context) throws IOException, InterruptedException {
        String[] words = inputValue.toString().split("\t");
        String word = words[0];
        String wordCount = words[1];
        String[] keys = word.split(" ");

        String firstString = keys[0];


        if(keys.length == 1){

            context.write(new StringPair(firstString,firstString),new Text(wordCount));
        }else{
            String secondString = keys[0]+" "+keys[1];
            context.write(new StringPair(firstString,secondString),inputValue);
        }



       /* String biGramDetails = value[1];
        String uniGramDetails = value[2];
        String uniGramWord  = value[2].split(" ")[0];

        context.write(new Text(uniGramWord),new Text(value[1] + "\t"+value[2]));
*/

    }

}
