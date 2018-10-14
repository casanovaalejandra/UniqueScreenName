package edu.uprm.cse.bigdata;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//The object of this MapReduce job is to select the unique scree names from a set of 100k tweets, the screen name is the field in the
//tweet that the user can change, it is the name that the person selects as name of the user, no the same as the username selected when creating the account
//thi field is open to repetition because there is no restriction when putting it.

public class UniqueScreenNameDriver {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: Count number of occurance of specific words<input path> <output path>");
            System.exit(-1);
        }
        Job job = new Job();
        job.setJarByClass(edu.uprm.cse.bigdata.UniqueScreenNameDriver.class);
        job.setJobName("Count Word Occurance");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(edu.uprm.cse.bigdata.UniqueScreenNameMapper.class);
        job.setCombinerClass(edu.uprm.cse.bigdata.UniqueScreenNameReducer.class);
        job.setReducerClass(edu.uprm.cse.bigdata.UniqueScreenNameReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
