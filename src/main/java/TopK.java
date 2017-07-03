/**
 * Created by shayan on 6/29/17.
 */
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.util.*;

class Pair {
    Text pid;
    Long time;

    Pair(Text pid, Long time) {
        this.pid = pid;
        this.time = time;
    }
}

public class TopK {

    private static Comparator<Pair> pairComparator = new Comparator<Pair>(){
        public int compare(Pair left, Pair right) {
            if (left.time > right.time) {
                return 1;
            }
            return -1;
        }
    };

    private static void AddtoQueue(PriorityQueue<Pair> Q, Pair pair, int k){
        if (Q.size() < k) {
            Q.add(pair);
        } else {
            Pair peak = Q.peek();
            if (pairComparator.compare(pair, peak) > 0) {
                Q.poll();
                Q.add(pair);
            }
        }
    }


    public static class TopKMapper extends Mapper<Object, Text, Text, LongWritable> {
        private PriorityQueue<Pair> Q;
        private int k;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            k = Integer.parseInt(conf.get("k"));
            Q = new PriorityQueue<Pair>(k, pairComparator);
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            String[] row = line.split("\t");
            Pair pair = new Pair(new Text(row[0]), Long.parseLong(row[1]));
            AddtoQueue(Q, pair, k);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // Output our 100 records to the mapper
            Iterator<Pair> iter = Q.iterator();
            while (iter.hasNext()) {
                Pair result = iter.next();
                context.write(result.pid, new LongWritable(result.time));
            }
        }
    }

    public static class TopKReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        private PriorityQueue<Pair> Q;
        private int k;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            k = Integer.parseInt(conf.get("k"));
            Q = new PriorityQueue<Pair>(k, pairComparator);
        }

        @Override
        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            Text pid = new Text(key.toString());
            Long value = 0L;
            for (LongWritable item : values) {
                value = item.get();
            }

            Pair pair = new Pair(pid, value);
            AddtoQueue(Q, pair, k);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // Output our 100 records to the mapper
            Iterator<Pair> iter = Q.iterator();
            while (iter.hasNext()) {
                Pair current = iter.next();
                context.write(current.pid, new LongWritable(current.time));
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.set("k",args[2]);

        Job job = Job.getInstance(conf);

        job.setMapperClass(TopKMapper.class);
        job.setReducerClass(TopKReducer.class);

        job.setJarByClass(TopK.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        TextInputFormat.setInputPaths(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}