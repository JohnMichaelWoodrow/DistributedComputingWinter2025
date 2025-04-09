package org.example;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Mapper class for counting occurrences of movie ratings in a dataset.
 *
 * This class processes input data line by line, where each line is expected to have at least three fields.
 * The third field is assumed to be a numeric rating between 1 and 5.
 * Valid ratings are emitted as keys with a count of 1.
 * Uses Hadoop's built-in logging framework to catch exceptions.
 *
 */


public class WordCountMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
    private IntWritable rating = new IntWritable();
    private final static IntWritable one = new IntWritable(1);
    private static final Log LOG = LogFactory.getLog(WordCountMapper.class);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\\s+"); // Split by whitespace
        if (fields.length >= 3) { // Ensure the line has at least 3 fields
            try {
                int movieRating = Integer.parseInt(fields[2]); // Parse the 3rd field as rating
                if (movieRating >= 1 && movieRating <= 5) { // Only consider valid ratings
                    rating.set(movieRating);
                    context.write(rating, one); // Emit rating and count 1
                }
            } catch (NumberFormatException e) {
                LOG.warn("Skipping line due to invalid rating: " + value.toString(), e);
            }
        }
    }
}
