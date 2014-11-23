package com.pat.tugas2;

import java.io.*;
import java.util.*;

import javax.xml.parsers.*;

import org.xml.sax.*;
import org.xml.sax.helpers.*;

public class Parser {
   
   private final int maxAuthorsPerPaper = 200;
   
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
            }
            
        }

        public void endElement(String namespaceURI, String localName,
                String rawName) throws SAXException {
            if(insidePerson){
            	Value += 1;
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
   
   Parser(String uri) {
      try {
	     SAXParserFactory parserFactory = SAXParserFactory.newInstance();
	     SAXParser parser = parserFactory.newSAXParser();
	     ConfigHandler handler = new ConfigHandler();
         parser.getXMLReader().setFeature(
	          "http://xml.org/sax/features/validation", true);
         parser.parse(new InputSource(new StringReader(uri)), handler);
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


