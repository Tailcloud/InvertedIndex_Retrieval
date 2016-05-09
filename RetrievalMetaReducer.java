package hw1.retrieval;

import java.io.IOException;

import java.text.*;
import java.util.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;

public class RetrievalMetaReducer extends Reducer<Text, TermWeighting, Text, Text> {
	int docCount=0;
	Hashtable<String, Integer> fileWordCountTbl = new Hashtable<String, Integer>();
	NumberFormat formatter = new DecimalFormat("0.0000000000000");
	CmdParser cmdParser;
	
	public void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		cmdParser = new CmdParser(conf.get("cmd"));
	}
	
	public void reduce(Text key, Iterable<TermWeighting> values, Context context) throws IOException, InterruptedException {
		StringTokenizer tokens = new StringTokenizer(key.toString(), " ");
		String fileId = tokens.nextToken();
		
		if(fileId.startsWith("!")){
			// special values for counting doc number and word number of files
			for(TermWeighting term : values) {
				countWord(term.getOffsets(), term.getTf());
			}
			docCount = fileWordCountTbl.size();
		} else {
			String output;
			String word = tokens.nextToken();
			for(TermWeighting term : values) {
				if(cmdParser.isQueryWord(word)) {
					Double tw = term.calTermWeighting(docCount, fileWordCountTbl.get(fileId));
					output = formatter.format(tw).toString()+";"+word+";"+term.toString();
					context.write(new Text(fileId), new Text(output));
				}
			}
		}
	}
	
	private void countWord(String fileId, Integer tf){
		Integer wordCount = fileWordCountTbl.get(fileId);
		if(wordCount == null){
			wordCount = new Integer(tf);
		} else {
			wordCount += tf;
		}
		fileWordCountTbl.put(fileId, wordCount);
	}
	
}