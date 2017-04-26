package cisc.mapreduce;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Analysis {


	public static class InputMapper
	extends Mapper<Object, Text, Text, IntWritable>{

		private final static IntWritable one = new IntWritable(1);
		private final static IntWritable minus = new IntWritable(-1);
		private final Text Prod = new Text();
		private final Set <String> PosWords = new HashSet<String>();
		private final Set <String> NegWords = new HashSet<String>();
		
		 @Override
		 //Setup distributed cache, store txt files in HashSets
			protected void setup(Context context) throws IOException, InterruptedException {
				try{
					URI Lists[]  = context.getCacheFiles();
					Path PosList = new Path(Lists[0]);
					Path NegList = new Path(Lists[1]);

					readFilePos(PosList);
					readFileNeg(NegList);


				} 
				catch(IOException e) {
					System.err.println("Exception in DistributedCache Setup: " + e.getMessage());
				}
			}
		 	//Following method stores positive words in PosWords hash set
			private void readFilePos(Path filePath){		
				try{
					System.out.print(filePath.toString());
					BufferedReader br = new BufferedReader(new FileReader(filePath.toString()));
					String word = null;
					while ((word = br.readLine()) != null){
							PosWords.add(word);
						
					}
				}
				catch(IOException x){
					System.err.println("File Read Error");
				}
			}
			//Following method stores negative words in PosWords hash set
			private void readFileNeg(Path filePath){		
				try{
					System.out.print(filePath.toString());
					BufferedReader br = new BufferedReader(new FileReader(filePath.toString()));
					String word = null;
					while ((word = br.readLine()) != null){
							NegWords.add(word); 
						
					}
				}
				catch(IOException x){
					System.err.println("File Read Error");
				}

			}
		    
		public void map(Object key, Text value, Context context
				) throws IOException, InterruptedException {
			String[] Review = value.toString().split("\t");

			String ProdID = Review[1];						//Product ID second Element in split
			String body = Review[7];						//Body 7th Element in split
			String[] ReviewBody = body.split("\\s+");		//Split on white space
			Prod.set(ProdID);
			
			for (String words : ReviewBody){
				words = words.replaceAll("[^a-zA-Z0-9\']","").toLowerCase();	//replace non alphanumeric or ' characters with blank string. Convert to lower case
				if (PosWords.contains(words)){			//check if word exists in positive list
					context.write(Prod, one);			//write +1 
				}
				else if (NegWords.contains(words)){		//check if word exists in negative list
					context.write(Prod, minus);			//write -1
				}
			}
		}
	}

	public static class AnalyzeReducer
	extends Reducer<Text,IntWritable,Text,Text> {
		private Text result = new Text();

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context
				) throws IOException, InterruptedException {
			  int sum = 0;
			  //Iterate through score and sum
		      for (IntWritable val : values) {
		        sum += val.get();
		      }
		      //Compute Sentiment Result
		      if (sum > 0){
		    	  result.set("positive");
		      }
		      else if (sum < 0){
		    	  result.set("negative");
		      }
		      else{
		    	  result.set("neutral");
		      }
		      context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Sentiment Analysis");
		job.addCacheFile(new Path("positive.txt").toUri());
		job.addCacheFile(new Path("negative.txt").toUri());
		job.setJarByClass(Analysis.class);
		job.setMapperClass(InputMapper.class);
		//job.setCombinerClass(AnalyzeReducer.class);
		job.setReducerClass(AnalyzeReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}