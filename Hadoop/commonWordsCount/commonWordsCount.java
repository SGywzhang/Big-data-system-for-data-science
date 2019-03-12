import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;


public class CommonWords {
	
	//Remove Stopwords
    public static class TokenizerWCMapper extends Mapper<Object, Text, Text, IntWritable> {

        Set<String> stopwords = new HashSet<String>();

        @Override
        protected void setup(Context context) {
            try {
                Path path = new Path("/Task1_data/stopwords.txt");
                FileSystem fs = FileSystem.get(new Configuration());
                BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
                String word = null;
                while ((word = br.readLine()) != null) {
                    stopwords.add(word);
                }

            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
 
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

        	StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                if (stopwords.contains(word.toString()))
                    continue;
                context.write(word, one);
            }
        }
    }
 
    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable count = new IntWritable();
 
        public void reduce(Text key, Iterable<IntWritable> values,
                Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            count.set(sum);
            context.write(key, count);
        }
    }
    
    //Mapper1
    public static class Mapper1 extends Mapper<Text, Text, Text, Text> {

        private Text frequency = new Text();

        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            //ToDo
            frequency = value;
            context.write(key,new Text(frequency+"_S1"));
        }
    }

 	//Mapper2
    public static class Mapper2 extends Mapper<Text, Text, Text, Text> {
        private Text frequency = new Text();
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            //ToDo
            frequency = value;
            context.write(key,new Text(frequency+"_S2"));
        }
    }
     
    //Get the number of common words reduce
    public static class CountCommonReducer extends Reducer<Text, Text, Text, IntWritable> {
        private IntWritable commoncount = new IntWritable();
 
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
                //ToDo

                ArrayList<Integer> comWordList = new ArrayList<Integer>();

                for (Text val : values) {
                    String[] freqInfo = val.toString().split("_");
                    comWordList.add(Integer.parseInt(freqInfo[0]));
                }

                if(comWordList.size()==1) return;

                commoncount = new IntWritable(Collections.min(comWordList));
                context.write(key, commoncount);
            }
        }
    // redefine the compartor for the sorting
    private static class IntWritableDecreasingComparator extends IntWritable.Comparator {

        public int compare(WritableComparable a, WritableComparable b) {
            return -super.compare(a, b);
        }
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return -super.compare(b1, s1, l1, b2, s2, l2);
        }
    }

   //sort the result
    public static class SortMapper extends Mapper<Text, Text, IntWritable, Text> {

        private IntWritable count = new IntWritable();

        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
    	    //ToDo
            count.set(Integer.parseInt(value.toString()));
            context.write(count,key);
        }
    }
    private static int count = 0;
    public static class SortReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

        public void reduce(IntWritable key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
            //ToDo
            for(Text val : values){
                if(count == 15)
                    break;
                context.write(key, val);
                count++;
                System.out.println("count : "+count);
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        conf.set("fs.defaultFS", "hdfs://172.16.81.252:9000/");
        conf.set("mapreduce.job.jar", "target/BigDataAssignment1Task1-1.0-SNAPSHOT.jar");
        conf.set("mapreduce.framework.name", "yarn");
        conf.set("yarn.resourcemanager.hostname", "172.16.81.252");
        conf.set("mapreduce.app-submission.cross-platform", "true");
        conf.set("mapreduce.framework.name", "yarn");
        conf.set("mapreduce.jobhistory.address", "172.16.81.252:10020");
        conf.set("mapreduce.jobhistory.webapp.address", "172.16.81.252:19888");

        String[] ioArgs=new String[]{"-Dmapreduce.job.queuename =default",

                "/Task1_data/task1-input1.txt",
                "/Task1_data_output/Task1_data_input1_stop_out/",

                "/Task1_data/task1-input2.txt",
                "/Task1_data_output/Task1_data_input2_stop_out/",

                "/Task1_data_output/Task1_data_common_count_out/",
                "/Task1_data_output/Task1_data_sort_out/"};


        String[] otherArgs = new GenericOptionsParser(conf, ioArgs).getRemainingArgs();
        
        if (otherArgs.length != 6) {
            System.err
                    .println("Usage: CommonWords <input1> <output1> <input2> <output2> "
                            + "<output3> <output4>");
            System.exit(2);
        }
        
        Job job1 = new Job(conf, "WordCount1");
        //Job job1 = Job.getInstance(conf);
        job1.setJarByClass(CommonWords.class);
        job1.setMapperClass(CommonWords.TokenizerWCMapper.class);
        job1.setCombinerClass(IntSumReducer.class);
        job1.setReducerClass(IntSumReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        job1.setNumReduceTasks(1);
        FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job1, new Path(otherArgs[1]));
        job1.waitForCompletion(true);
 
        Job job2 = new Job(conf, "WordCount2");
        job2.setJarByClass(CommonWords.class);
        job2.setMapperClass(CommonWords.TokenizerWCMapper.class);
        job2.setCombinerClass(IntSumReducer.class);
        job2.setReducerClass(IntSumReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        job2.setNumReduceTasks(1);
        FileInputFormat.addInputPath(job2, new Path(otherArgs[2]));
        FileOutputFormat.setOutputPath(job2, new Path(otherArgs[3]));
        job2.waitForCompletion(true);

        Job job3 = new Job(conf, "Count words in common");
        job3.setJarByClass(CommonWords.class);
        job3.setReducerClass(CommonWords.CountCommonReducer.class);
        MultipleInputs.addInputPath(job3, new Path(otherArgs[1]),
                                    KeyValueTextInputFormat.class, CommonWords.Mapper1.class);
        MultipleInputs.addInputPath(job3, new Path(otherArgs[3]),
                                    KeyValueTextInputFormat.class, CommonWords.Mapper2.class);
 
        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(Text.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(IntWritable.class);
        job3.setNumReduceTasks(1);
        FileOutputFormat.setOutputPath(job3, new Path(otherArgs[4]));
        job3.waitForCompletion(true);
 
        Job job4 = new Job(conf, "sort");
        job4.setJarByClass(CommonWords.class);
        job4.setInputFormatClass(KeyValueTextInputFormat.class);
        job4.setSortComparatorClass(IntWritableDecreasingComparator.class);
        job4.setMapperClass(SortMapper.class);
        job4.setReducerClass(SortReducer.class);
        job4.setOutputKeyClass(IntWritable.class);
        job4.setOutputValueClass(Text.class);
        job4.setMapOutputKeyClass(IntWritable.class);
        job4.setMapOutputValueClass(Text.class);
        job4.setNumReduceTasks(1);
        FileInputFormat.addInputPath(job4, new Path(otherArgs[4]));
        FileOutputFormat.setOutputPath(job4, new Path(otherArgs[5]));
        System.exit(job4.waitForCompletion(true) ? 0 : 1);
    }
}

