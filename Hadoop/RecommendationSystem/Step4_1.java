
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
//
//import HDFSAPI;

public class Step4_1 {
    public static class Step4_PartialMultiplyMapper extends Mapper<LongWritable, Text, Text, Text> {

        // you can solve the co-occurrence Matrix/left matrix and score matrix/right matrix separately
        private String flag; //@ywzhang04022019

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            FileSplit split = (FileSplit) context.getInputSplit();
            flag = split.getPath().getParent().getName();// data set //@ywzhang04022019
        }

        @Override
        public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
            String[] tokens = Recommend.DELIMITER.split(values.toString());
            //ToDo
            //@ywzhang04022019
            //System.out.println(flag);
            if(flag.equals("step3_1")){  //score matrix

                String[]  user_score = tokens[1].split(":");
                String itemID =  tokens[0];
                String userID = user_score[0];
                String score = user_score[1];

                Text k = new Text(itemID);
                Text v = new Text("A:"+userID+","+score);

                context.write(k,v);
            }
            else if(flag.equals("step3_2")){ // co-occurence matrix

                String[] item1_item2_ID = tokens[0].split(":");
                String itemID_1 = item1_item2_ID[0];
                String itemID_2 = item1_item2_ID[1];
                String subTotalNum = tokens[1];

                Text k = new Text(itemID_1);
                Text v = new Text("B:"+itemID_2+","+subTotalNum);

                context.write(k,v);
            }
        }
    }

    public static class Step4_AggregateReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //ToDo
            //@ywzhang04022019



            Map<String,String> mapScoreMatrix = new HashMap<String,String>();
            Map<String,String> mapCooccurrenceMatrix = new HashMap<String,String>();

            for(Text v:values){

                String val = v.toString();

                if(val.startsWith("A")){// userID Score
                    String[] kv = Recommend.DELIMITER.split(val.substring(2));
                    mapScoreMatrix.put(kv[0],kv[1]);
                }
                else if(val.startsWith("B")){ //itemID_2 subTotalNum
                    String[] kv = Recommend.DELIMITER.split(val.substring(2));
                    mapCooccurrenceMatrix.put(kv[0],kv[1]);
                }
            }
            double result = 0.0;
            Iterator<String> iter = mapCooccurrenceMatrix.keySet().iterator();

            while(iter.hasNext()){
                String mapCMKey =  iter.next();

                int num = Integer.parseInt(mapCooccurrenceMatrix.get(mapCMKey));

                Iterator<String> iter2 =  mapScoreMatrix.keySet().iterator();

                while(iter2.hasNext()){
                    String mapScoreKey = iter2.next();

                    double score = Double.parseDouble(mapScoreMatrix.get(mapScoreKey));

                    result = num * score;

                    Text k = new Text(mapScoreKey.toString());
                    Text v = new Text(mapCMKey+","+result);

                    context.write(k,v);
                }
            }
        }
    }

    public static void run(Map<String, String> path) throws IOException, InterruptedException, ClassNotFoundException {
        //get configuration info
        Configuration conf = Recommend.config();
        // get I/O path
        Path input1 = new Path(path.get("Step4_1Input1"));
        Path input2 = new Path(path.get("Step4_1Input2"));
        Path output = new Path(path.get("Step4_1Output"));
        // delete last saved output
        HDFSAPI hdfs = new HDFSAPI(new Path(Recommend.HDFS));
        hdfs.delFile(output);
        // set job
        Job job = Job.getInstance(conf);
        job.setJarByClass(Step4_1.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(Step4_PartialMultiplyMapper.class);
        job.setReducerClass(Step4_AggregateReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, input1, input2);
        FileOutputFormat.setOutputPath(job, output);

        job.waitForCompletion(true);
    }
}


