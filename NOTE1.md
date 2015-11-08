HOW Map and Reduce Works?
====

It takes me a lot of time to seperate the items for `city.txt`, this part is quite challenging for me because I missed two things:

1) how `StringTokenizer` works in Java

2) how Map and execute each time.


## How `StringTokenizer` works

This part is easy for me now. I check the source code of this built-in class, and find the defautl delim is `\t\r\f\n`, so if by default we passed a sentence and construct a tokenizer, when calling `nextToken()` function, it will **return the whole line**.

That's what the tokenizer works.

## How Map and Reduce Work

After I fixed the first issue in [Recap] package, I can manulplate in quite simple java code, but in MapReduce, I still failed to parse the city.txt

I find that I have some misunderstanding of how Hadoop calls Map.

### My previous understanding

I think when calling Map(), each time it passed a **whole** file's content to this function, so that we need to parse each line with "\n", then parse each item in the line with ",".

But it turns out that I was wrong, because when I adds the following code:

```
		@Override
		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			
			String line = value.toString();
			// for debugging, print out each line
			System.out.println(line);
			
			StringTokenizer tokenizerArticle = new StringTokenizer(line);
			
			// treat each line
			while (tokenizerArticle.hasMoreTokens()) {
				
				// get the content of each line
				String eachLine = tokenizerArticle.nextToken();
				
				System.out.println("each line: " + eachLine);
				
				// split by space
				StringTokenizer tokenizerLine = new StringTokenizer(eachLine, ",");
				
				strID = tokenizerLine.nextToken();
				if(tokenizerLine.hasMoreTokens()) {
					strCode = tokenizerLine.nextToken();
					
					if (tokenizerLine.hasMoreTokens()) {
						strDistrict = tokenizerLine.nextToken();
						
						if (tokenizerLine.hasMoreTokens()) {
							strPopulation = tokenizerLine.nextToken();

						}
					}
				}
				
				
				Text name = new Text(strID);
				int scoreInt = Integer.parseInt(strPopulation);
				output.collect(name, new IntWritable(scoreInt));
			}

			
		}	
	}
```

The output only occurs one time "each line: XXX", and the second it goes to `hasMoreTokens()`, it still outputs the first line in `city.txt`

I thought this question is quite similar to my previous bug in python when I write the file and don't move the cursor.

**So I asked, why?**

Finally I goes back to the sample in `HelloWorld`, each time when I learned something I always find that it's quick to write notes and simple examples, here
I benefit a lots.


I add more logs in the example since it runs well, and I finally use this code and its output to figure out how `map()` and `reduce()` works.

```
		private static int count = 0;
		
		
		@Override
		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			count += 1;
			// get the input string from the file
			String line = value.toString();
			System.out.println("line " + count +  ": " + line);
			// treat each string and split by space by default
			StringTokenizer tokenizer = new StringTokenizer(line);
			// get words and do iteratively
			while (tokenizer.hasMoreTokens()) {
				// for each word, eject (word, 1) as one item, for example ("Hello", 1)
				word.set(tokenizer.nextToken());
				output.collect(word, one);
			}
		}	
	}
```

For one file like `file01` or `file02`, I noticed that multiple-lines file has executed the `map()` function multiple times, which means that in fact when executing it, each time it just passed a single line of the file to the map().

In my previous assumption, I have tokenize the new line, which is unnessary, because for each new line, each time I just need to separate by `,`, that's stupid for me to do extra work and it will result in error, since we don't have new line for one `map()` calling


And for the above code sample, then why then can divide the line? because the default delim can divide by space, change-line etc... so even though at first they're a whole line, they can still use the same tokenizer to split using the while loop.


## Tricks

Pay attention to how `map()` and `reduce()` works can save a lot of time when coding, even though I find they're easy after spending a lot of time reading the books.


## Implement

For StringTokenizer, here are the default delims: the space character, the tab character, the newline character, the carriage-return character, and the form-feed character.



