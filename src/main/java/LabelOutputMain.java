import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

/**
 * Created by fk on 17-7-15.
 */
public class LabelOutputMain {



    public static class LabelOutputMapper extends Mapper<Object,Text,IntWritable,Text>
    {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String str = value.toString();
            String name = str.split("\t")[0];
            int label = Integer.parseInt(str.split("\t")[1]);
            context.write(new IntWritable(label),new Text(name));
        }
    }

    public static class LabelOutputReducer extends Reducer<IntWritable,Text,IntWritable,Text>
    {
        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String result = "";
            for(Text text:values){
                result += text.toString() + " ";
            }
            context.write(key,new Text(result));
        }
    }


}
