package hw1.invertedIndex;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.*;

public class WordValue implements Writable {
	private StringBuffer terms = new StringBuffer("");
	private int count = 0;

	public WordValue(){
	}
	  
	public WordValue(String term){
		terms.append(term);
		count = 1;
	}
	  
	public String getTerms(){
		return terms.toString();
	}
	  
	public int getCount(){
		return count;
	}
	  
	public void addTerm(String term){
		terms.append(term);
		count++;
	}
	
	public void addWordValue(WordValue wv){
		terms.append(wv.getTerms());
		count += wv.getCount();
	}
	
	public void sortTerms() {
		String[] termsArr = terms.toString().split(";");
		TermString[] tsArr = new TermString[termsArr.length];
		for(int i=0; i<termsArr.length; i++) {
			tsArr[i] = new TermString(termsArr[i]);
		}
		Arrays.sort(tsArr);
		terms = new StringBuffer(tsArr[0].toString());
		for(int i=1; i<tsArr.length; i++) {
			terms.append(";"+tsArr[i].toString());
		}
	}
	class TermString implements Comparable {
		int value;
		String term;
		
		public TermString (String term) {
			value = Integer.parseInt(term.substring(0, term.indexOf(" ")));
			this.term = term;
		}
		
		public String toString() {
			return term;
		}
		
		public int compareTo(Object o1){
			if(this.value>((TermString)o1).value){
				return 1;
			}else{
				return -1;
			}
		}
	}
	@Override
	public void write(DataOutput out) throws IOException {
		new Text(terms.toString()).write(out);
		out.writeInt(count);
	}
	  
	@Override
	public void readFields(DataInput in) throws IOException {
		Text text = new Text();
		text.readFields(in);
		this.terms = new StringBuffer(text.toString());
		this.count = in.readInt();
	}

	@Override
	public String toString(){
		sortTerms();
		return count+" "+terms.toString();
	}
}