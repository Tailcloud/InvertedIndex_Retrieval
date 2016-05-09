package hw1.retrieval;

import java.io.IOException;

import java.util.regex.*;
import java.util.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;

public class RetrievalMapper extends Mapper<LongWritable, Text, Text, FileScore> {
	String word="";
	String offsets="";
	String fileId="";
	double tw;
	CmdParser cmdParser;
	
	public void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		cmdParser = new CmdParser(conf.get("cmd"));
	}
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		StringTokenizer tokens = new StringTokenizer(value.toString(), " \t;");
		fileId = tokens.nextToken();
		tw = Double.parseDouble(tokens.nextToken());
		word = tokens.nextToken();
		String fsoffs = tokens.nextToken();
		offsets = fsoffs.substring(1, fsoffs.length()-1);
		if(cmdParser.isQueryWord(word))
			context.write(new Text(fileId), new FileScore(word, offsets, tw));
	}
}