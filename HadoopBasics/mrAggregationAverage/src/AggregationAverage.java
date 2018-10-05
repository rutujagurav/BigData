import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AggregationAverage {

    public static class SumCount implements Writable {
        long sum;
        long count;

        public void write(DataOutput out) throws IOException {
            out.writeLong(sum);
            out.writeLong(count);
        }

        public void readFields(DataInput in) throws IOException {
            sum = in.readLong();
            count = in.readLong();
        }

        @Override
        public String toString() {
            return String.format("%f", (float)sum/count);
        }
    }

    public static class TokenizerMapper extends Mapper<Object, Text, IntWritable, SumCount> {
        private IntWritable outKey = new IntWritable();
        private SumCount outVal = new SumCount();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            if (((LongWritable) key).get() != 0) {
                int statusCode = Integer.parseInt(value.toString().split("\t")[5]);
                int length = value.getLength();
                outKey.set(statusCode);
                outVal.sum = length;
                outVal.count = 1;
                context.write(outKey, outVal);
            }
        }
    }

    public static class IntSumReducer extends Reducer<IntWritable,SumCount, IntWritable, SumCount> {

        public void reduce(IntWritable key, Iterable<SumCount> values, Context context ) throws IOException, InterruptedException {
            SumCount total = new SumCount();
            for(SumCount v : values) {
                total.count += v.count;
                total.sum += v.sum;
            }

            context.write(key,total);

        }
    }

    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf,"avgAggregation");
        job.setJarByClass(AggregationAverage.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(SumCount.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setNumReduceTasks(4);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}

