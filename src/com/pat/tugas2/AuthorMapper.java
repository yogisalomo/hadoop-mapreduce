package com.pat.tugas2;

import java.io.IOException;
import java.io.StringReader;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.jdom2.JDOMException;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.Locator;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;
import org.xml.sax.helpers.DefaultHandler;


public class AuthorMapper extends Mapper<LongWritable,Text,Text,DoubleWritable>{
	private final int maxAuthorsPerPaper = 200;
	private Context ctx;
	
	   public class ConfigHandler extends DefaultHandler {

	        private Locator locator;

	        private Integer Value;
	        private String key;
	        private String recordTag;
	        private int numberOfPersons = 0;

	        private boolean insidePerson;
	        private boolean isYear;
	        

	        public void setDocumentLocator(Locator locator) {
	            this.locator = locator;
	        }
	        
	        public Integer getValue(){
	        	return Value;
	        }
	        
	        public String getKey(){
	        	return key;
	        }
	        
	        public void startElement(String namespaceURI, String localName,
	                String rawName, Attributes atts) throws SAXException {
	            
	            insidePerson = (rawName.equals("author") || rawName.equals("editor"));
	            isYear = rawName.equals("year");
	            if ((atts.getLength()>0) && ((atts.getValue("key"))!=null)) {
	                Value = 0; // Reset the value of The Author
	                key = "";
	                recordTag = rawName;
	            }
	            
	        }

	        public void endElement(String namespaceURI, String localName,
	                String rawName) throws SAXException {
	            if(insidePerson){
	            	Value += 1;
	            }
	            if(rawName.equals(recordTag)){
	            	if(key == "" || key == null){
	            		key = "undefined";
	            	}
	            	
	            	try {
						ctx.write(new Text(key), new DoubleWritable(Value));
	            	 } catch (IOException ex) {
	                     Logger.getLogger(AuthorMapper.class.getName()).log(Level.SEVERE, null, ex);
	                 } catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
	            }
	        }

	        public void characters(char[] ch, int start, int length)
	                throws SAXException {
	            if (isYear){
	            	key = new String(ch, start, length);
	            }
	        }

	        private void Message(String mode, SAXParseException exception) {
	            System.out.println(mode + " Line: " + exception.getLineNumber()
	                    + " URI: " + exception.getSystemId() + "\n" + " Message: "
	                    + exception.getMessage());
	        }

	        public void warning(SAXParseException exception) throws SAXException {

	            Message("**Parsing Warning**\n", exception);
	            throw new SAXException("Warning encountered");
	        }

	        public void error(SAXParseException exception) throws SAXException {

	            Message("**Parsing Error**\n", exception);
	            throw new SAXException("Error encountered");
	        }

	        public void fatalError(SAXParseException exception) throws SAXException {

	            Message("**Parsing Fatal Error**\n", exception);
	            throw new SAXException("Fatal Error encountered");
	        }
	    }
	
	@Override
    protected void map(LongWritable key, Text xml, Context context) throws IOException, InterruptedException 	{
        
		//System.out.println("Map is Executed");
		//System.out.println("Processing this XML: ");
		//System.out.println(xml.toString());
        ctx = context;
        
        //Parse XML kemudian emit per tahun ( key ) dengan jumlah author (value) 
        try {
   	     	String doc = "<?xml version=\"1.0\" encoding=\"ISO-8859-1\"?><!DOCTYPE dblp SYSTEM \"/home/hduser/dblp.dtd\">"
   	     			+ "<dblp>"+ xml.toString() + "</dblp>";
            SAXParserFactory parserFactory = SAXParserFactory.newInstance();
   	        SAXParser parser = parserFactory.newSAXParser();
   	        ConfigHandler handler = new ConfigHandler();
            parser.getXMLReader().setFeature(
   	          "http://xml.org/sax/features/validation", true);
            parser.parse(new InputSource(new StringReader(doc)), handler);
         } catch (IOException e) {
            System.out.println("Error reading URI: " + e.getMessage());
         } catch (SAXException e) {
            System.out.println("Error in parsing: " + e.getMessage());
         } catch (ParserConfigurationException e) {
            System.out.println("Error in XML parser configuration: " +
   			    e.getMessage());
         }
      }
}
