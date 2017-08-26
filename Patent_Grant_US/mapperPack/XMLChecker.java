package mapperPack;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.xml.sax.EntityResolver;
import org.xml.sax.InputSource;

public class XMLChecker {

	EntityResolver resolver = new EntityResolver () {
		public InputSource resolveEntity (String publicId, String systemId) {
			String empty = "";
			ByteArrayInputStream bais = new ByteArrayInputStream(empty.getBytes());
			System.out.println("resolveEntity:" + publicId + "|" + systemId);
			return new InputSource(bais);
		}
	};
	
	public void parseXML(Text input,Text pub_type, LongWritable days ) throws Exception, IOException
	{
		DocumentBuilderFactory docFactory=DocumentBuilderFactory.newInstance();
		DocumentBuilder dBuilder=docFactory.newDocumentBuilder();
		dBuilder.setEntityResolver(resolver);
		
		Document doc=dBuilder.parse(input.toString());
		doc.getDocumentElement().normalize();
		
		XPath xPath = XPathFactory.newInstance().newXPath();
		pub_type =new Text(((Node) xPath.evaluate("/us-bibliographic-data-application/publication-reference/document-id/@kind", doc, XPathConstants.NODE)).getNodeValue());
		String pub_date = ((Node) xPath.evaluate("/us-bibliographic-data-application/publication-reference/document-id/@date", doc, XPathConstants.NODE)).getNodeValue();
		String appl_date = ((Node) xPath.evaluate("/us-bibliographic-data-application/application-reference/document-id/@date", doc, XPathConstants.NODE)).getNodeValue();
		
		days = new LongWritable(ChronoUnit.DAYS.between(LocalDate.parse(pub_date, DateTimeFormatter.BASIC_ISO_DATE), LocalDate.parse(appl_date, DateTimeFormatter.BASIC_ISO_DATE)));
		
		
	}
}
