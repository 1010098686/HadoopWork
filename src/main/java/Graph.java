/**
 * Created by rajesh on 17-7-11.
 */
import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Graph {
    public static class MyMapper extends Mapper<Object, Text, Text, IntWritable> {

        private Text link = new Text();
        private IntWritable times = new IntWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if(line.length() > 0) {
                String tmp = line.split("\t")[0];
                tmp = tmp.substring(1,tmp.length()-1);
                link.set(tmp);
                times.set(Integer.parseInt(line.split("\t")[1]));
                context.write(link, times);
            }
        }
    }

    public static class MyCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable total = new IntWritable();
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for(IntWritable v:values) {
                sum += v.get();
            }
            total.set(sum);
            context.write(key, total);
        }
    }

    public static class MyPartitioner extends Partitioner<Text, IntWritable> {

    @Override
    public int getPartition(Text key, IntWritable intWritable, int i) {
        String character = key.toString().split(",")[0];
        Text tmp = new Text(character);
        return (tmp.hashCode() & Integer.MAX_VALUE) % i;
    }
}

    public static class MyReducer extends Reducer<Text, IntWritable, Text, Text> {
        private Text character = new Text();
        private Text link = new Text();

        static Text currentCharacter = new Text(" ");
        static List<String> outList = new ArrayList<String>();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            String[] tmp = key.toString().split(",");
            character.set(tmp[0]);
            for(IntWritable v:values) {
                sum += v.get();
            }
            link.set(tmp[1] + "," + sum);
            if(!currentCharacter.equals(character) && !currentCharacter.equals(new Text(" "))) {
                StringBuilder out = new StringBuilder();
                long total = 0;
                for(String o:outList) {
                    total += Long.parseLong(o.substring(o.indexOf(",")+1));
                }

                out.append("[");
                for(String o:outList) {
                    double f = (double)Long.parseLong(o.substring(o.indexOf(",")+1)) / (double)total;
                    out.append(o.split(",")[0]);
                    out.append(",");
                    out.append(f);
                    out.append("|");
                }
                out.replace(out.length()-1,out.length(),"]");
                String s = out.toString();
                context.write(currentCharacter,new Text(s));

                outList = new ArrayList<String>();
            }
            currentCharacter = new Text(character);
            outList.add(link.toString());
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            StringBuilder out = new StringBuilder();
            long total = 0;
            for(String o:outList) {
                total += Long.parseLong(o.substring(o.indexOf(",")+1));
            }

            out.append("[");
            for(String o:outList) {
                double f = (double)Long.parseLong(o.substring(o.indexOf(",")+1)) / total;
                out.append(o.split(",")[0]);
                out.append(",");
                out.append(f);
                out.append("|");
            }
            out.replace(out.length()-1,out.length(),"]");
            String s = out.toString();
            context.write(currentCharacter,new Text(s));
        }
    }

}
