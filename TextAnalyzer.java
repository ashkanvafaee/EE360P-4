import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.join.TupleWritable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.TestPureJavaCrc32.Table;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;




// Do not change the signature of this class
public class TextAnalyzer extends Configured implements Tool {

    // Replace "?" with your own output key / value types
    // The four template data types are:
    //     <Input Key Type, Input Value Type, Output Key Type, Output Value Type>
    public static class TextMapper extends Mapper<LongWritable, Text, Text, Text> {
    	
    	// value = line to be processed
    	public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException
        {   		
    		
            // Implementation of you mapper function
    		String text = value.toString();
    		text = text.toLowerCase();
    		text = text.replaceAll("[^a-zA-Z0-9]", " ");    
    		text = text.replaceAll("\\s+", " ");
    		text = text.replaceAll("\n", " ").replaceAll("\r", " ");
    		text.trim();
	
    		ArrayList<String> history = new ArrayList<>();	
    		String [] words = text.split("\\s+");
	    		
    		for(int i = 0; i < words.length; i++) {
    			String query_word = new String();
    			query_word = words[i].trim();
    			if(!history.contains(words[i])) {
        			history.add(words[i]);
        			boolean first_flag = true;
        			for(int j = 0; j < words.length; j++) {
        				String context_word = new String();
        				context_word = words[j];
        				
        				
        				
        				if(!query_word.equals(context_word)) {
        					Text query_word_text = new Text(query_word);
        					Text context_word_text = new Text(context_word);
        					if((query_word_text.getLength() > 0) && context_word_text.getLength() > 0) {
                			
            					context.write(query_word_text, context_word_text);

        					}
        				}
        				else {
        					if(first_flag) {
        						first_flag = false;
        					}
        					else {
            					Text query_word_text = new Text(query_word);
            					Text context_word_text = new Text(context_word);
            					if((query_word_text.getLength() > 0) && context_word_text.getLength() > 0) {
                    			
                					context.write(query_word_text, context_word_text);

            					}
        						
        						
        					}
        				}

        			}
    			}
    		}
    		
    		
    		
        }
    }

    // Replace "?" with your own key / value types
    // NOTE: combiner's output key / value types have to be the same as those of mapper
    public static class TextCombiner extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
               
            throws IOException, InterruptedException
        {
        	HashMap <String, Integer> map = new HashMap<>();
        	String s = new String();
        	for(Text t : values) {
        		s += t.toString() + " ";
        	}
        	
        	String [] words = s.split("\\s+");
        	
        	for(int i=0; i<words.length; i++) {
        		if(map.containsKey(words[i])){
        			map.replace(words[i], map.get(words[i]) + 1);
        		}
        		else {
        			map.put(words[i], new Integer(1));
        		}
        	}


        	
        	String temp = new String();
        	for(String str : map.keySet()) {
        		String name = str;
        		String num = String.valueOf((Integer) map.get(str));
        		temp += name + " " + num + " ";
        	}
        	
        	Text result = new Text(temp);

        
        	if((key.getLength() > 0) && (result.getLength() > 0)) {
                context.write(key, result);
        	}
        	
        	
            // Implementation of you combiner function
        }
    }

    // Replace "?" with your own input key / value types, i.e., the output
    // key / value types of your mapper function
    public static class TextReducer extends Reducer<Text, Text, Text, Text> {
        private final static Text emptyText = new Text("");

       

        public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException
        {
        	HashMap <String, Integer> map = new HashMap<>();
        	
        	Comparator<String> ordered_string = new Comparator<String>() {
            	public int compare(String s1, String s2) {
            		int result;
            			result = String.CASE_INSENSITIVE_ORDER.compare(s1, s2);
            			if(result == 0) {
            				result = s1.compareTo(s2);
            			}
            		

            		return result;
            	}
            };
   
        	
            // Implementation of you reducer function

            // Write out the results; you may change the following example
            // code to fit with your reducer function.
            //   Write out the current context key
            
            //   Write out query words and their count
            
        	
        	
        	String s = new String();
        	for(Text t : values) {
        		if(t.getLength() > 0) {
            		s += t.toString();
        		}
        	}
        	
        	String [] words = s.split("\\s+");
        	
        	for(int i=0; i<words.length; i = i + 2) {
        		String name = words[i];
        		String num = words[i + 1];
        		
           		if(map.containsKey(name)){
        			map.replace(name, map.get(name) + Integer.valueOf(num));
        		}
        		else {
        			map.put(name, Integer.valueOf(num));
        		}
        		
        	}
        	

        	ArrayList<String> ordered_words = new ArrayList<>();
        	for(String str: map.keySet()) {
        		ordered_words.add(str);
        	}
        	Collections.sort(ordered_words, ordered_string);
        	
        	// Finding value for Context Word
        	int max = 0;
        	ArrayList<String> max_query = new ArrayList<>();
        	
        	for(String str : ordered_words) {
        		if(map.get(str) > max) {
        			max = map.get(str);
        		}
        	}
        	
        	for(String str : ordered_words) {
        		if(map.get(str) == max) {
        			max_query.add(str);
        		}
        	}
        	
        	for(String str : max_query) {
        		ordered_words.remove(str);
        	}
        	        	
        	// Omitting all empty keys
        	if(key.getLength() > 0) {            	
        		context.write(key, new Text(String.valueOf(max)));
        		
        		//writing max_query word next
        		for(String str : max_query) {
            		context.write(new Text("<" + str + ","), new Text(String.valueOf(max) + ">"));
        		}
        		
            	for(String str : ordered_words) {
            		Text word = new Text("<" + str + ",");
            		Text val = new Text(new String(String.valueOf((Integer) map.get(str))) + ">");
            		
            		if((word.getLength() > 0) && val.getLength() > 0) {
            			context.write(word, val);
            		}
            	}
        	}
        	            

            /*for(String queryWord: map.keySet()){
                String count = map.get(queryWord).toString() + ">";
                queryWordText.set("<" + queryWord + ",");
                context.write(queryWordText, new Text(count));
            }*/
            //   Empty line for ending the current context key
            context.write(emptyText, emptyText);
            map.clear();
        }
    }
    
    

    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();

        // Create job
        Job job = new Job(conf, "av28837_rg36763"); // Replace with your EIDs
        job.setJarByClass(TextAnalyzer.class);

        // Setup MapReduce job
        job.setMapperClass(TextMapper.class);
        //   Uncomment the following line if you want to use Combiner class
        job.setCombinerClass(TextCombiner.class);
        job.setReducerClass(TextReducer.class);
        
        // Changed here
       // job.setMapOutputKeyClass(Text.class);
        //job.setMapOutputValueClass(MapWritable.class);  
        //

        // Specify key / value types (Don't change them for the purpose of this assignment)
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //   If your mapper and combiner's  output types are different from Text.class,
        //   then uncomment the following lines to specify the data types.
        //job.setMapOutputKeyClass(Text.class);
        //job.setMapOutputValueClass(MapWritable.class);

        // Input
        FileInputFormat.addInputPath(job, new Path(args[0]));
        job.setInputFormatClass(TextInputFormat.class);

        // Output
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setOutputFormatClass(TextOutputFormat.class);

        // Execute job and return status
        return job.waitForCompletion(true) ? 0 : 1;
    }

    // Do not modify the main method
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new TextAnalyzer(), args);
        System.exit(res);
    }

    // You may define sub-classes here. Example:
    // public static class MyClass {
    //
    // }
}
