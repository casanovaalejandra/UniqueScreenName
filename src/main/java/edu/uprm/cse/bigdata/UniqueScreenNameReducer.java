package edu.uprm.cse.bigdata;

import com.google.common.collect.Iterables;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;

//This class will count all the occurrences of the key, which is the screen name.
// THe algortihm is once the count of each screen_name is done, if screen_name has >1 then it is not unique, therefore it will be discarded.
public class UniqueScreenNameReducer extends Reducer<Text,IntWritable,Text,IntWritable> {

    protected void reduce(Text key, Iterable<IntWritable> values, Reducer.Context context)
            throws IOException, InterruptedException {
        //counter
        int count = 0;

        // iterator over list of 1s, to count them (no size() or length() method available
        //Here we count the number of occurrences of the names
        for (IntWritable value : values ){
            count++;
        }
        if(Iterables.size(values)<2){
            context.write(key, new IntWritable(count));
        }
        // emit key-pair: key, count
        // key is the word
        // count is the number of times that word appeared on the tweets
        Logger logger = LogManager.getRootLogger();


    }
}
