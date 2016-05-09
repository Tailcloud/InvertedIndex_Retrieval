package hw1.invertedIndex;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class WordRankPartitioner extends Partitioner<Text, WordValue> {
	@Override
	public int getPartition(Text key, WordValue value, int numReduceTasks) {
		
		if(key.toString().substring(0,1).matches("^[A-Ga-g]")){
			return 0;
		} else if(key.toString().substring(0,1).matches("^[H-Zh-z]")){
			return 1;
		} else 
		  return 0;
	}
}