package hw1.retrieval;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.*;

public class TermWeighting implements Writable {
	private Text offsets;
	private int df;
	private int tf;

	public TermWeighting() {
		offsets = new Text();
	}
	
	public TermWeighting(int df, int tf, String offsets) {
		this.df = df;
		this.tf = tf;
		this.offsets = new Text(offsets);
	}
	
	public String getOffsets(){
		return offsets.toString();
	}
	
	public int getDf() {
		return df;
	}
	
	public int getTf() {
		return tf;
	}
	
	public Double calTermWeighting(int docCount, int wordCount) {
		double termFeq = (double)tf/wordCount;
		double invDocFeq = ( Math.log( (double)wordCount/df ) );
		return new Double( termFeq*invDocFeq );
	}
	
	@Override
	public String toString() {
		return offsets.toString();
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(df);
		out.writeInt(tf);
		offsets.write(out);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		df = in.readInt();
		tf = in.readInt();
		offsets.readFields(in);
	}
}