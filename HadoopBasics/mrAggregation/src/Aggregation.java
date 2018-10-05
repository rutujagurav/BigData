import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Aggregation {

    public static class TokenizerMapper extends Mapper<Object, Text, IntWritable, IntWritable>{
        //private final static IntWritable one = new IntWritable(1);
        private IntWritable outKey = new IntWritable();
        private IntWritable outVal = new IntWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            if (((LongWritable)key).get() != 0) {

                int statusCode = Integer.parseInt(value.toString().split("\t")[5]); //response code like "200", "304", etc
                int length = value.getLength(); //the whole row's length
                outKey.set(statusCode);
                outVal.set(length);
                context.write(outKey, outVal);
            }
        }
    }

    public static class IntSumReducer extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable>{
        private IntWritable result = new IntWritable();

        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
            int sum = 0;
            for(IntWritable val : values){
                sum += val.get();
            }
            result.set(sum);
            context.write(key,result);
        }
    }

    public static void main(String args[]) throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf,"aggregation");
        job.setJarByClass(Aggregation.class);
        job.setMapperClass(TokenizerMapper.class); //Mapper
        job.setCombinerClass(IntSumReducer.class); //Combiner is the reducer
        job.setReducerClass(IntSumReducer.class);  //Reducer
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setNumReduceTasks(2);
        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}
