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

public class RetrievalMetaMapper extends Mapper<LongWritable, Text, Text, TermWeighting> {
	String word="";
	String offsets="";
	String fileId="";
	int tf=0;
	int df=0;
	Hashtable<String, Integer> fileWordCountTbl = new Hashtable<String, Integer>();
	CmdParser cmdParser;
	
	public void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		cmdParser = new CmdParser(conf.get("cmd"));
	}
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		StringTokenizer tokens = new StringTokenizer(value.toString(), " \t;");
		word = tokens.nextToken();
		if(cmdParser.isIgnore()){
			word = word.toLowerCase();
		}
		df = Integer.parseInt(tokens.nextToken());
		for(int i=0; i<df; i++){
			fileId = tokens.nextToken();
			tf = Integer.parseInt(tokens.nextToken());
			offsets = tokens.nextToken();
			
			TermWeighting tw = new TermWeighting(df, tf, offsets);
			context.write(new Text(fileId+" "+word), tw);
			
			countWord(fileId, tf);
		}
	}
	
	public void cleanup(Context context) throws IOException, InterruptedException {
		for(String fileId : fileWordCountTbl.keySet()) {
			Integer wordCount = fileWordCountTbl.get(fileId);
			TermWeighting count = new TermWeighting(0, wordCount, fileId);
			context.write(new Text("!doc"), count);
		}
	}
	
	private void countWord(String fileId, int tf){
		Integer wordCount = fileWordCountTbl.get(fileId);
		if(wordCount == null){
			wordCount = new Integer(tf);
		} else {
			wordCount += tf;
		}
		fileWordCountTbl.put(fileId, wordCount);
	}
	
}