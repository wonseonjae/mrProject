package cache;

import lombok.extern.log4j.Log4j;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

@Log4j
public class WordCount3Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    public void reduce(Text Key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
        int wordCount = 0;

        for (IntWritable value : values) {
            wordCount += value.get();
        }
        context.write(Key, new IntWritable(wordCount));
    }

}
