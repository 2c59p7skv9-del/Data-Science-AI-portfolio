package gr.aueb.panagiotisl.mapreduce.wordcount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class WordCount {

    /**
         * Custom intermediate value type (Writable) that flows through:
        * Mapper -> Combiner -> Reducer.
     * 
     * It stores everything needed to compute, per (country, month):
     *  - sumDance: sum of danceability scores (for average)
     *  - count: number of songs (for average)
     *  - maxDance + maxSong: the most danceable song and its score
     */
    public static class DanceStatsWritable implements Writable {
        private double sumDance;
        private long count;
        private double maxDance;
        private String maxSong;

        // Hadoop requires a no-arg constructor for Writables
        public DanceStatsWritable() {
            this.sumDance = 0.0;
            this.count = 0L;
            this.maxDance = Double.NEGATIVE_INFINITY;
            this.maxSong = "";
        }

        public DanceStatsWritable(double sumDance, long count, double maxDance, String maxSong) {
            this.sumDance = sumDance;
            this.count = count;
            this.maxDance = maxDance;
            this.maxSong = (maxSong == null) ? "" : maxSong;
        }

        public double getSumDance() { return sumDance; }
        public long getCount() { return count; }
        public double getMaxDance() { return maxDance; }
        public String getMaxSong() { return maxSong; }

        // Serialization (Writable contract)
        @Override
        public void write(DataOutput out) throws IOException {
            out.writeDouble(sumDance);
            out.writeLong(count);
            out.writeDouble(maxDance);
            out.writeUTF(maxSong);
        }

        // Deserialization (Writable contract)
        @Override
        public void readFields(DataInput in) throws IOException {
            sumDance = in.readDouble();
            count = in.readLong();
            maxDance = in.readDouble();
            maxSong = in.readUTF();
        }
    }

    // --------- MAPPER ----------
    public static class CountMapper extends Mapper<LongWritable, Text, Text, DanceStatsWritable> {

        private final Text outKey = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();

            //  Ignore CSV header line
            if (line.startsWith("\"spotify_id\"") || line.startsWith("spotify_id")) {
                return;
            }

            //  Split CSV safely: commas inside double-quotes are NOT treated as separators
            String[] tokens = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);

            //  Column indices based on the provided CSV header
            final int IDX_NAME = 1;
            final int IDX_COUNTRY = 6;
            final int IDX_SNAPSHOT_DATE = 7;   // YYYY-MM-DD
            final int IDX_DANCEABILITY = 13;   //  numeric in [0,1]

            if (tokens.length <= IDX_DANCEABILITY) return;

            String songName = stripQuotes(tokens[IDX_NAME]);
            String country = stripQuotes(tokens[IDX_COUNTRY]);
            String snapshotDate = stripQuotes(tokens[IDX_SNAPSHOT_DATE]);
            String danceStr = stripQuotes(tokens[IDX_DANCEABILITY]);

            // 4) Basic validation
            if (country == null || country.isEmpty()) return;
            if (snapshotDate == null || snapshotDate.length() < 7) return; // need YYYY-MM
            if (danceStr == null || danceStr.isEmpty()) return;

            double danceability;
            try {
                danceability = Double.parseDouble(danceStr);
            } catch (NumberFormatException e) {
                return;
            }

            //  Month extraction: YYYY-MM
            String month = snapshotDate.substring(0, 7);

            //  Emit Key: "COUNTRY:YYYY-MM"
            outKey.set(country + ":" + month);

            //  Value: partial stats for this row
            // sumDance=danceability, count=1, maxDance=danceability, maxSong=songName
            context.write(outKey, new DanceStatsWritable(danceability, 1L, danceability, songName));
        }
        // Remove surrounding quotes from CSV fields if present
        private String stripQuotes(String s) {
            if (s == null) return null;
            s = s.trim();
            if (s.length() >= 2 && s.startsWith("\"") && s.endsWith("\"")) {
                return s.substring(1, s.length() - 1);
            }
            return s;
        }
    }

    // --------- COMBINER ----------
    /**
     * Combiner aggregates intermediate values locally (per mapper)
     * reducing network traffic during shuffle
     * 
     * Safe to use because the aggregation operations are associative & commutative:
     * sumDance: sum
     * count: sum
     * maxDance/maxSong: max with deterministic tie-break
     */
    public static class CountCombiner extends Reducer<Text, DanceStatsWritable, Text, DanceStatsWritable> {
        @Override
        public void reduce(Text key, Iterable<DanceStatsWritable> values, Context context)
                throws IOException, InterruptedException {

            double sum = 0.0;
            long count = 0L;
            double maxDance = Double.NEGATIVE_INFINITY;
            String maxSong = "";

            for (DanceStatsWritable v : values) {
                sum += v.getSumDance();
                count += v.getCount();

                // Track the maximum danceability (and its song name)
                if (v.getMaxDance() > maxDance) {
                    maxDance = v.getMaxDance();
                    maxSong = v.getMaxSong();
                } else if (v.getMaxDance() == maxDance) {
                    // deterministic tie-break for stability: choose lexicographically smaller song name
                    if (v.getMaxSong() != null && !v.getMaxSong().isEmpty() &&
                            (maxSong == null || maxSong.isEmpty() || v.getMaxSong().compareTo(maxSong) < 0)) {
                        maxSong = v.getMaxSong();
                    }
                }
            }

            context.write(key, new DanceStatsWritable(sum, count, maxDance, maxSong));
        }
    }

    // --------- REDUCER ----------
    public static class CountReducer extends Reducer<Text, DanceStatsWritable, Text, Text> {

        private final Text outVal = new Text();

        @Override
        public void reduce(Text key, Iterable<DanceStatsWritable> values, Context context)
                throws IOException, InterruptedException {

            double sum = 0.0;
            long count = 0L;
            double maxDance = Double.NEGATIVE_INFINITY;
            String maxSong = "";

            // Aggregate across all values for this (country, month)
            for (DanceStatsWritable v : values) {
                sum += v.getSumDance();
                count += v.getCount();

                if (v.getMaxDance() > maxDance) {
                    maxDance = v.getMaxDance();
                    maxSong = v.getMaxSong();
                } else if (v.getMaxDance() == maxDance) {
                    // Same deterministic tie-break as combiner
                    if (v.getMaxSong() != null && !v.getMaxSong().isEmpty() &&
                            (maxSong == null || maxSong.isEmpty() || v.getMaxSong().compareTo(maxSong) < 0)) {
                        maxSong = v.getMaxSong();
                    }
                }
            }
            // Compute average danceability
            double avg = (count == 0) ? 0.0 : (sum / count);

            // Output format: "Song Name: max, avg: avg"
            outVal.set(String.format("%s: %.3f, avg: %.3f", maxSong, maxDance, avg));
            context.write(key, outVal);
        }
    }
}
