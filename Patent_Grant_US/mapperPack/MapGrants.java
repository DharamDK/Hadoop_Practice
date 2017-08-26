package mapperPack;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.StringReader;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.xml.sax.EntityResolver;
import org.xml.sax.InputSource;

public class MapGrants extends Mapper<LongWritable, Text, Text, IntWritable>{

	EntityResolver resolver = new EntityResolver () {
		public InputSource resolveEntity (String publicId, String systemId) {
			String empty = "";
			ByteArrayInputStream bais = new ByteArrayInputStream(empty.getBytes());
			System.out.println("resolveEntity:" + publicId + "|" + systemId);
			return new InputSource(bais);
		}
	};
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{		
		String xml=value.toString().trim();
		if(xml.contains("<?xml"))
		{
			try{
				
				DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
				DocumentBuilder dBuilder = factory.newDocumentBuilder();
				
				dBuilder.setEntityResolver(resolver);
				
				Document doc=dBuilder.parse(new InputSource(new StringReader(xml)));
				
				//System.out.println(doc.getDocumentElement().getNodeName());
				XPath xPath = XPathFactory.newInstance().newXPath();	
				Element appl= (Element) xPath.compile("/us-patent-application/us-bibliographic-data-application/application-reference/document-id/date").evaluate(doc, XPathConstants.NODE);
				
				if(appl!=null)
				{
					String applDate=appl.getTextContent();
					
					Node pub= (Node) xPath.compile("/us-patent-application/us-bibliographic-data-application/publication-reference/document-id").evaluate(doc, XPathConstants.NODE);
					
					if(pub!=null)
					{
						String kind = ((Element) pub).getElementsByTagName("kind").item(0).getTextContent();
						String pubDate = ((Element) pub).getElementsByTagName("date").item(0).getTextContent();
						
						System.out.println("Key : "+key+"\nPub Date : "+pubDate+"\nAppl Date : "+applDate);
						
						if (applDate!=null && pubDate!=null && kind!=null)
						{
							int days = (int) ChronoUnit.DAYS.between(LocalDate.parse(applDate, DateTimeFormatter.BASIC_ISO_DATE), LocalDate.parse(pubDate, DateTimeFormatter.BASIC_ISO_DATE));
							
							System.out.println("Days : "+days);
							
							if(days>0)
							{
								context.write(new Text(kind), new IntWritable(days));
							}
						}
					}
				}
				
			
			}
			catch(Exception e)
			{
				System.out.println("Something is wrong with XML !!!");
				e.printStackTrace();
			}
		}
		else
		{
			System.out.println("No XML Found !!!");
		}
	}
	
}
