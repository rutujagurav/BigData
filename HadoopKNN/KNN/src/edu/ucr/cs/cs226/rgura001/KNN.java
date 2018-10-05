package edu.ucr.cs.cs226.rgura001;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


public class KNN {

    public static class DistanceMapper extends Mapper<Object, Text, DoubleWritable, Text>{


        private DoubleWritable outKey = new DoubleWritable();
        private  Text outValue = new Text();
        private String X;
        private String Y;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);

            this.X = context.getConfiguration().get("X");
            this.Y = context.getConfiguration().get("Y");
        }


        public void map(Object key, Text value, Context context) throws IOException, InterruptedException, NullPointerException {

            //given point
            double x = Double.parseDouble(X);
            double y = Double.parseDouble(Y);

            //line from dataset, get x and y co-ordinates
            String ID = value.toString().split(",")[0];
            double x1 = Double.parseDouble(value.toString().split(",")[1]);
            double y1 = Double.parseDouble(value.toString().split(",")[2]);

            //Calculate distance
            double distance = Math.sqrt((Math.pow((x-x1),2)) + Math.pow((y-y1),2));

            outKey = new DoubleWritable(distance);
            outValue = new Text(ID);

            //Mapper emits <key,value> as <distance,ID>
            context.write(outKey,outValue);

        }

    }

    public static class kReducer extends Reducer<DoubleWritable, Text, DoubleWritable, Text>{


        private Text result = new Text();
        private int k, count;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            this.k = Integer.parseInt(context.getConfiguration().get("K"));
            this.count = 0;
        }

        public void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            if(count < k) {
                for (Text val : values)
                    result.set(val);
                context.write(key, result); //Reducer emits <distance,ID> for k nearest points
                count++;
            }
        }

    }


    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();


        System.out.println("Hello...");

        Job job = Job.getInstance(conf, "knn");
        job.setJarByClass(KNN.class);
        job.setInputFormatClass(TextInputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.getConfiguration().set("K",args[2]);
        job.getConfiguration().set("X",args[3]);
        job.getConfiguration().set("Y",args[4]);

        job.setMapperClass(DistanceMapper.class);
        job.setCombinerClass(kReducer.class);
        job.setReducerClass(kReducer.class);
        job.setNumReduceTasks(1);
        job.setOutputKeyClass(DoubleWritable.class);
        job.setOutputValueClass(Text.class);



        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}


