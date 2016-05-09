package hw1.invertedIndex;

import java.io.IOException;

import java.util.regex.*;
import java.util.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Mapper;


public class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, WordValue> {
	int fileId;
	int tf;
	String word;
	String offsets;

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		StringTokenizer tokens = new StringTokenizer(value.toString(), " \t");
		fileId = Integer.parseInt(tokens.nextToken());
		word = tokens.nextToken();
		tf = Integer.parseInt(tokens.nextToken());
		offsets = tokens.nextToken();
		
		Text wk = new Text(word);
		WordValue wv = new WordValue(fileId+" "+tf+" "+offsets);
		context.write(wk, wv);
	}
}
