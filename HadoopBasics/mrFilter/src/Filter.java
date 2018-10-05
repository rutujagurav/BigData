import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Filter {

    public static class TokenizerMapper extends Mapper<Object, Text, NullWritable, Text>{
        private final static IntWritable one = new IntWritable(1);
        private String filterText;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            this.filterText = context.getConfiguration().get("filter-text");
        }
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String statusCode = value.toString().split("\t")[5];
            if (statusCode.equals(filterText))
                context.write(NullWritable.get(), value);
        }


    }


    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf,"filter");
        job.setJarByClass(Filter.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        Path input = new Path(args[0]);
        FileInputFormat.addInputPath(job,input);
        Path output = new Path(args[1]);
        FileOutputFormat.setOutputPath(job, output);
        String status = args[2];
        job.getConfiguration().set("filter-text",status);
        System.exit(job.waitForCompletion(true) ? 0 : 1);


    }

}
