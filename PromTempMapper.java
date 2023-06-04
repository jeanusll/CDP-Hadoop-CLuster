import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PromTempMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
    private static final DoubleWritable temp = new DoubleWritable(1);
    private Text year = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
         String line = value.toString();
         String str[] = line.split(" ");

        if (str.length > 5){

                year.set(str[0]);

                temp.set(Double.parseDouble(str[4]));

        }

        context.write(year, temp);

    }
}
