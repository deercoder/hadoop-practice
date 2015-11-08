package com.deercoder.recap;

import java.util.StringTokenizer;

public class Test {

	// for this experiment, I want to know StringTokenizer so that I can parse well the city.txt
	public static void main(String[] args) {

		String line = "1,2,3,3,4\n2,3,3,4,5\n5,5,5,6,7";
		String word = "";
		
		// by default, Java StringTokenizer uses "\n\t\f\r"
		StringTokenizer tokenizer = new StringTokenizer(line);

		// so this while loop judges by the default delim
		while (tokenizer.hasMoreTokens()) {
			
			// Since it's divided by default, so each word is by "\n", it's a Sentence for each line
			word = new String(tokenizer.nextToken());
			System.out.println(word.toString());
			
			// here the word is "1,2,3,3,4", not just one element like "1", becuase it delims by default using "\n"
			// We still need other delims to separate each item in a line
			StringTokenizer tokenizerLine = new StringTokenizer(word, ",");
			while (tokenizerLine.hasMoreTokens()) {
				System.out.println(tokenizerLine.nextToken());
			}
			
		}

	}

}
