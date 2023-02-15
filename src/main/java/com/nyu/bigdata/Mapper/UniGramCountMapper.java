package com.nyu.bigdata.Mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

;import java.io.IOException;

public class UniGramCountMapper extends Mapper<LongWritable, Text,Text, Text> {
    private final static String DEFAULT_VALUE = "Default_Value";
    private final Logger logger = LoggerFactory.getLogger(UniGramCountMapper.class);
    @Override
    public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {


        String line = value.toString();
        if(line.length() < 3){
            logger.debug("The length of line is {} which is less than 3",line.length());
            return;
        }

        String lowerCaseLine = line.replaceAll("[^a-zA-Z0-9]", " ").trim().replaceAll(" +"," ").toLowerCase().trim();

        String[] words = lowerCaseLine.split(" ");
        for(int i=0;i<words.length;i++){
            String biGramWord = DEFAULT_VALUE;
            String biGramWordKey = DEFAULT_VALUE;
            if(i != words.length-1){
                biGramWord = words[i]+" "+words[i+1];
                biGramWordKey = words[i]+words[i+1];
            }
            String uniGramWord = words[i];


            String finalValue = biGramWord + " " + "1" + "\t"+uniGramWord+" "+"1";
            if(biGramWord.equalsIgnoreCase(DEFAULT_VALUE)){
                context.write(new Text(uniGramWord),new Text(finalValue));
            }
            else{
                context.write(new Text(biGramWordKey),new Text(finalValue));
            }

        }



    }

}

/*
--> unigram count bigram count
the cat 1 the 1 cat 2*/
