import org.ansj.domain.Term;
import org.ansj.library.UserDefineLibrary;
import org.ansj.splitWord.analysis.ToAnalysis;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by fk on 17-7-11.
 */
public class SplitMain {


    public static void addPersonName(Configuration conf, String filename) {
        String result = "";
        try {
            FileSystem fs = FileSystem.get(conf);
            BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(new Path(filename))));
            String line;
            while ((line = reader.readLine()) != null) {
                result = result + " " + line;
            }
            reader.close();
            conf.set("personName", result.trim());
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static class SplitMapper extends Mapper<Object, Text, Text, Text> {
        private List<String> nameList = null;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            String personName = context.getConfiguration().get("personName");
            nameList = new ArrayList<String>();
            String[] names = personName.split(" ");
            for (String name : names) {
                nameList.add(name);
                if (!UserDefineLibrary.contains(name)) {
                    UserDefineLibrary.insertWord(name, "nr", 1000); //nr表示词性为人名
                }
            }
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String str = value.toString();
            String out = "";
            List<Term> result = ToAnalysis.parse(str);
            for (Term term : result) {
                String nature = term.getNatureStr();
                String termName = term.getName();
                if (nature.equals("nr") && nameList.contains(termName)) {
                    out += " " + termName;
                }
            }
            out = out.trim();
            String outKey = ((FileSplit) context.getInputSplit()).getPath().getName();
            context.write(new Text(outKey), new Text(out));
        }
    }

    public static class SplitReducer extends Reducer<Text, Text, NullWritable, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String result = "";
            for (Text text : values) {
                String str = text.toString();
                if (!str.equals("")) {
                    result = result + str + "\n";
                }
            }
            result = result.trim();
            context.write(NullWritable.get(), new Text(result));
        }
    }
}
