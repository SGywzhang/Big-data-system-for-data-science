import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
//
//import HDFSAPI;

public class Step5 {
	public static class Step5_FilterSortMapper extends Mapper<LongWritable, Text, Text, Text> {
		private String flag;
		private Text k;
		private Text v;
        @Override
		protected void setup(Context context) throws IOException, InterruptedException {
        	 FileSplit split = (FileSplit) context.getInputSplit();
             flag = split.getPath().getParent().getName();
		}

		@Override
        public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
			//you can use provided SortHashMap.java or design your own code.
			//ToDo
			//
			//System.out.println("Step5"+flag);
			if(flag.equals("step4_2")){
				String[] tokens = Recommend.DELIMITER.split(values.toString());
				k = new Text(tokens[0]);
				v = new Text("W:" + tokens[1] + "," + tokens[2]);
			}else{
				String[] tokens = Recommend.DELIMITER.split(values.toString());
				k = new Text(tokens[0]);
				v = new Text("S:" + tokens[1] + "," + tokens[2]);
			}
			context.write(k,v);
		}

        
	}
	public static class Step5_FilterSortReducer extends Reducer<Text, Text, Text, Text> {

		private Text k;
		private Text v;
		@Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
        	//you can use provided SortHashMap.java or design your own code.

            //ToDo
			HashMap<String,String> wMap = new HashMap<String,String>();
			HashMap<String,String> sMap = new HashMap<String,String>();

			for(Text line:values){
				System.out.println(line);
				String[] tokens = Recommend.DELIMITER.split(line.toString());
				String flag = tokens[0].split(":")[0];
				String itemID = tokens[0].split(":")[1];
				if(flag.equals("W")){
					wMap.put(itemID, tokens[1]);
				}else{
					sMap.put(itemID, tokens[1]);
				}
			}
			HashMap<String,Float> filterMap = new HashMap<String,Float>();
			Iterator<String> iter = wMap.keySet().iterator();
			while(iter.hasNext()){
				String k = iter.next();
				if(sMap.containsKey(k)==false)
					filterMap.put(k, Float.valueOf(wMap.get(k)));
			}
			System.out.println(filterMap.isEmpty());

			List<Entry<String,Float>> list = new LinkedList<Entry<String,Float>>();
			list=SortHashMap.sortHashMap(filterMap);
			System.out.println("enter before");
			System.out.println(list.isEmpty());
			for(Entry<String,Float> l : list){
                if(key.equals(new Text("928"))){
                    k = key;
                    v = new Text(l.getKey() + "," + l.getValue().toString());
                    context.write(k,v);
                }
			}
        }
    }
	public static void run(Map<String, String> path) throws IOException, InterruptedException, ClassNotFoundException {
		//get configuration info
		Configuration conf = Recommend.config();
		// I/O path
		Path input1 = new Path(path.get("Step5Input1"));
		Path input2 = new Path(path.get("Step5Input2"));
		Path output = new Path(path.get("Step5Output"));
		// delete last saved output
		HDFSAPI hdfs = new HDFSAPI(new Path(Recommend.HDFS));
		hdfs.delFile(output);
		// set job
        Job job =Job.getInstance(conf);
        job.setJarByClass(Step5.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(Step5_FilterSortMapper.class);
        job.setReducerClass(Step5_FilterSortReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, input1,input2);
        FileOutputFormat.setOutputPath(job, output);

        job.waitForCompletion(true);
	}
}

