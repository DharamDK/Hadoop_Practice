import java.io.File;
import java.io.FileWriter;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.w3c.dom.Document;

import mapperPack.MapGrants;
import reducerPack.ReduceGrants;

// hadoop jar /home/dk/workspace/Patent.jar PatentGrantMain /input/ipa_xmls.txt /PatentGrantedCount

public class PatentGrantMain extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
		int exitCode = ToolRunner.run(new PatentGrantMain(), args);
		System.exit(exitCode);
	}

	@Override
	public int run(String[] arg0) throws Exception {
		// TODO Auto-generated method stub
		
		if(arg0.length<2)
		{
			System.out.println("Please provide input and output Directories Correctly !!!");
			return -1;
		}
		
		Configuration conf=new Configuration();
		Job job=Job.getInstance(conf,"Patent Grant");
		
	    job.setJarByClass(PatentGrantMain.class);
	    job.setMapperClass(MapGrants.class);
//	    job.setCombinerClass(CombineGrants.class);
	    job.setReducerClass(ReduceGrants.class);
		
	    FileInputFormat.setInputPaths(job, new Path(arg0[0]));
	    FileOutputFormat.setOutputPath(job, new Path(arg0[1]));
		
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(IntWritable.class);
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(FloatWritable.class);
	    
	    job.waitForCompletion(true);
		
		return 0;
	}
	
	public int checkXML(String[] arg0) throws Exception
	{	
		File input=new File(arg0[0]);
		
		
		if (input!=null)
		{
			File output=new File(input.getPath()+"/intermediate.txt");
			DocumentBuilderFactory docFactory=DocumentBuilderFactory.newInstance();
			DocumentBuilder dBuilder=docFactory.newDocumentBuilder();
			Document doc=dBuilder.parse(input);
			doc.normalizeDocument();

			output.setWritable(true);

			FileWriter opFile=new FileWriter(output);

			opFile.append(doc.getTextContent());
			opFile.close();
		}
		return 0;
		
	}

}
