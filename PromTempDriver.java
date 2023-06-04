import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;


public class PromTempDriver extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new PromTempDriver(), args);
        System.exit(exitCode);
    }

    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.printf("Usage: %s needs two arguments, input and output files\n", getClass().getSimpleName());
            return -1;
        }

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setJarByClass(PromTempDriver.class);
        job.setJobName("Calculo del promedio de Temperatura por Año");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setMapperClass(PromTempMapper.class);
        job.setReducerClass(PromTempReducer.class);

        int returnValue = job.waitForCompletion(true) ? 0:1;

        if(job.isSuccessful()) {
            System.out.println("Trabajo Realizado");
        } else if(!job.isSuccessful()) {
            System.out.println("Ocurriò un error");
        }

        return returnValue;
    }
}
