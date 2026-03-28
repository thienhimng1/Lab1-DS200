import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MovieRatingAnalysis {

    // ================= MAPPER =================
    public static class MovieMapper extends Mapper<Object, Text, Text, DoubleWritable> {
        
        // Từ điển lưu trữ: movieId -> MovieTitle
        private Map<String, String> movieTitleMap = new HashMap<>();
        
        private Text titleKey = new Text();
        private DoubleWritable ratingValue = new DoubleWritable();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 0) {
                File moviesFile = new File(cacheFiles[0].getPath());
                BufferedReader br = new BufferedReader(new FileReader(moviesFile));
                String line;
                
                while ((line = br.readLine()) != null) {
                    // Cắt chuỗi. Lưu ý kiểm tra lại dấu phân cách trong file của bạn (dấu , hay ::)
                    String[] parts = line.split(","); 
                    
                    if (parts.length >= 2) {
                        String movieId = parts[0].trim();
                        String title = parts[1].trim(); // Tên phim thường ở cột thứ 2
                        movieTitleMap.put(movieId, title);
                    }
                }
                br.close();
            }
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] parts = line.split(","); 
            
            if (parts.length >= 3) {
                try {
                    String movieId = parts[1].trim(); 
                    double rating = Double.parseDouble(parts[2].trim()); 

                    // Lấy tên phim từ từ điển
                    String title = movieTitleMap.get(movieId);

                    if (title != null) {
                        titleKey.set(title);
                        ratingValue.set(rating);
                        context.write(titleKey, ratingValue);
                    }
                } catch (NumberFormatException e) {
                    // Bỏ qua dòng lỗi hoặc header
                }
            }
        }
    }

    // ================= REDUCER =================
    public static class MovieReducer extends Reducer<Text, DoubleWritable, Text, Text> {
        
        // Khai báo biến lớp để tìm bộ phim có điểm cao nhất
        private double maxRating = -1.0;
        private String maxMovie = "";
        
        private Text resultValue = new Text();

        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double sum = 0;
            int count = 0;

            for (DoubleWritable val : values) {
                sum += val.get();
                count++;
            }

            if (count > 0) {
                double average = sum / count;
                
                // Ghi ra kết quả của từng phim
                String formattedOutput = String.format("AverageRating: %.2f (TotalRatings: %d)", average, count);
                resultValue.set(formattedOutput);
                context.write(key, resultValue);

                // Cập nhật thông tin phim có điểm cao nhất (chỉ xét phim có >= 5 lượt đánh giá)
                if (count >= 5 && average > maxRating) {
                    maxRating = average;
                    maxMovie = key.toString();
                }
            }
        }

        // Hàm cleanup chạy 1 lần duy nhất ở cuối cùng sau khi reduce() đã xử lý mọi thứ
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            if (!maxMovie.isEmpty()) {
                String bestMovieStr = String.format("is the highest rated movie with an average rating of %.2f among movies with at least 5 ratings.", maxRating);
                
                // In ra dòng tổng kết ở cuối file part-r-00000
                context.write(new Text("\n" + maxMovie), new Text(bestMovieStr));
            }
        }
    }

    // ================= DRIVER =================
    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: MovieRatingAnalysis <input_ratings_dir> <output_dir> <movies_file_path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Movie Rating Analysis");
        job.setJarByClass(MovieRatingAnalysis.class);

        job.setMapperClass(MovieMapper.class);
        job.setReducerClass(MovieReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Đưa file movies.txt vào Cache
        job.addCacheFile(new Path(args[2]).toUri());

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}