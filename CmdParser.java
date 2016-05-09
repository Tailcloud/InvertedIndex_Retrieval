package hw1.retrieval;

import java.util.*;

public class CmdParser {
	private String cmd;
	private String oper = ":or";
	private HashSet<String> words;
	
	public CmdParser(){
		oper = ":or";
		words = new HashSet<String>();
	}
	
	public CmdParser(String cmd) {
		this.cmd = cmd;
		words = new HashSet<String>();
		parseCmd();
	}
	
	public boolean isQueryWord(String word) {
		if(isIgnore())
			word = word.toLowerCase();
		return words.contains(word);
	}
	
	public boolean isIgnore() {
		return oper.equals(":ignore");
	}
	
	public String getCmd() {
		return cmd;
	}
	
	public Set<String> getWords(){
		return words;
	}
	
	public void parseCmd() {
		StringTokenizer tokens = new StringTokenizer(cmd, " +");
		String first = tokens.nextToken();
		if(parseOper(first)==false)
			words.add(first);
		while(tokens.hasMoreTokens()){
			String token = tokens.nextToken();
			if(isIgnore())
				token = token.toLowerCase();
			words.add(token);
		}
	}
	
	public void parseCmd(String cmd) {
		this.cmd = cmd;
		parseCmd();
	}
	
	public boolean parseOper(String first) {
		return (first.startsWith(":") && isOper(first));
	}
	
	private boolean isOper(String testWord) {
		String op = testWord.toLowerCase();
		switch (op) {
			case ":or":
			case ":and":
			case ":not":
			case ":ignore":
				this.oper = op;
				return true;
			default :
				return false;
		}
	}
}