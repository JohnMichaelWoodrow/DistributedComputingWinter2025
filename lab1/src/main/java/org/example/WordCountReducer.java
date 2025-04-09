package org.example;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer class for grouping counts of movie ratings.
 *
 * This class receives key value pairs, where the key is a rating and the value is the count of occurrences of that rating.
 * It sums up all counts for each rating and emits the rating along with its total count.
 *
 */


public class WordCountReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
    @Override
    protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get(); // Sum up counts for each rating
        }
        context.write(key, new IntWritable(sum)); // Emit rating and total count
    }
}
