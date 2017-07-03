/**
 * Created by shayan on 6/29/17.
 */
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;


public class DedupByPid {

    public static class DedupByPidMapper extends Mapper<Object, Text, Text, LongWritable> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            JSONParser parser = new JSONParser();
            try {
                Object obj = parser.parse(value.toString());
                JSONObject jsonObject = (JSONObject) obj;
                Text pid = new Text(jsonObject.get("pid").toString());
                JSONArray actionsList = (JSONArray) jsonObject.get("actions");

                Iterator i = actionsList.iterator();
                // take each value from the json array separately
                while (i.hasNext()) {
                    JSONObject innerObj = (JSONObject) i.next();
                    Long time = Long.valueOf(innerObj.get("time").toString());
                    context.write(pid, new LongWritable(time));
                }
            } catch (Exception e) {
                //if an exception raising during parsing, we can potentially log it.
                //print stack for now
                e.printStackTrace();
            }
        }
    }

    public static class DedupByPidReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

        @Override
        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            //find the max time with same pid
            Long max = 0L;
            for (LongWritable item : values) {
                if (item.get() > max){
                    max = item.get();
                }
            }
            LongWritable outValue = new LongWritable(max);
            context.write(key, outValue);
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setMapperClass(DedupByPidMapper.class);
        job.setReducerClass(DedupByPidReducer.class);
        job.setJarByClass(DedupByPid.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        TextInputFormat.setInputPaths(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}