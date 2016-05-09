package hw1.invertedIndex;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.*;

public class Term implements Writable {
	private StringBuffer offsets = new StringBuffer("");
	private int count = 0;

	public Term(){
	}
  
	public Term(int offset){
		offsets.append(offset);
		count = 1;
	}
  
	public String getOffsets(){
		return offsets.toString();
	}
  
	public int getCount(){
		return count;
	}
  
	public void addOffset(int off){
		if(offsets.length()!=0)
			offsets.append(","+off);
		else
			offsets.append(off);
		count++;
	}
  
	public void addOffset(Term term){
		if(offsets.length()!=0)
			offsets.append(","+term.getOffsets());
		else
			offsets.append(term.getOffsets());
		count += term.getCount();
	}
  
	public void addOffset(String off, int count){
		if(offsets.length()!=0)
			offsets.append(","+off);
		else
			offsets.append(off);
		this.count += count;
	}
	
	public void sortOffsets(){
		String[] offs = offsets.toString().split(",");
		Arrays.sort(offs);
		offsets = new StringBuffer();
		for(int i=0; i<offs.length; i++) {
			if(offsets.length()!=0)
				offsets.append(","+offs[i]);
			else
				offsets.append(offs[i]);
		}
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		Text text = new Text(offsets.toString());
		text.write(out);
		out.writeInt(count);
	}
  
	@Override
	public void readFields(DataInput in) throws IOException {
		Text text = new Text();
		text.readFields(in);
		this.offsets = new StringBuffer(text.toString());
		this.count = in.readInt();
	}

	@Override
	public String toString(){
		sortOffsets();
		return count+" ["+offsets.toString()+"];";
	}
}
