package com.pat.tugas2;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;


/**
* Reads records that are delimited by a specifc begin/end tag.
*/
public class XmlInputFormat extends  TextInputFormat {

  public static final String START_TAG_KEY = "xmlinput.start";
 

    @Override
    public RecordReader<LongWritable,Text> createRecordReader(InputSplit is, TaskAttemptContext tac)  {
        return new XmlRecordReader();        
    }
    
    public static class XmlRecordReader extends RecordReader<LongWritable,Text> {
        private  byte[] Tag;
        private String currentTag;
        private ArrayList<String> listTag = new ArrayList<String>();    
        private  long start;
        private  long end;
        private  FSDataInputStream fsin;
        private  DataOutputBuffer buffer = new DataOutputBuffer();
        private LongWritable key = new LongWritable();
        private Text value = new Text();



            @Override
            public void initialize(InputSplit is, TaskAttemptContext tac) throws IOException, InterruptedException {
                FileSplit fileSplit= (FileSplit) is;
                Tag = tac.getConfiguration().get(START_TAG_KEY).getBytes("utf-8");
                //Insert the Tag bytes into list of Tag
                String tempString = "";
                for(int i=0; i< Tag.length;i++){
                    char temp = (char)Tag[i];
                    if(temp =='|'){
                            listTag.add(tempString);
                            tempString = ""; //Neutralize the Temporary String
                    }
                    else{
                            tempString+= Character.toString(temp);
                    }
                }
                
                //Checking the Tag -> DONE!
                //for(int i=0;i< listTag.size();i++){
                //	System.out.println(listTag.get(i));
                //}

                start = fileSplit.getStart();
                end = start + fileSplit.getLength();
                Path file = fileSplit.getPath();

                FileSystem fs = file.getFileSystem(tac.getConfiguration());
                fsin = fs.open(fileSplit.getPath());
                fsin.seek(start);

            }

            @Override
            public boolean nextKeyValue() throws IOException, InterruptedException {
                 if (fsin.getPos() < end) {
                     //System.out.println("nextkeyvalue called");
            if (readUntilNeededTag()) {
            	//System.out.println("readuntilneedtag return true");
              try {
                String endString = "</"+currentTag+">";
                //System.out.println("Looking for End String: "+endString);

                if (readUntilMatch(endString.getBytes("utf-8"))) {
                    //System.out.println("Read Until Match return True");
                        //System.out.println("The value set to value :");
                        //System.out.println(buffer.getData().toString());
                    	//System.out.println("The key set :");
                    	//System.out.println(fsin.getPos());
                        value.set(buffer.getData(), 0, buffer.getLength());
                        key.set(fsin.getPos());
                        return true;
                }
              } finally {
                buffer.reset();
              }
            }
          }
          return false;
            }

            @Override
            public LongWritable getCurrentKey() throws IOException, InterruptedException {
            return key;
            }

            @Override
            public Text getCurrentValue() throws IOException, InterruptedException {
                       return value;



            }

            @Override
            public float getProgress() throws IOException, InterruptedException {
                return (fsin.getPos() - start) / (float) (end - start);
            }

            @Override
            public void close() throws IOException {
                fsin.close();
            }
            private boolean readUntilMatch(byte[] match) throws IOException {
                  int i = 0;
                  //System.out.println("Length of EndTag array of byte: "+match.length);
                  //for(int j=0;j<match.length;j++){
                  //	System.out.print((char)match[j]);
                  //}
                  //System.out.println();
                  
                  while (true) {
                    int b = fsin.read();
                    
                    //System.out.println("Read The Character "+ Character.toString((char)b));
                    // end of file:
                    if (b == -1) return false;
                    // save to buffer:
                    buffer.write(b);
                    // check if we're matching:
                    if (b == match[i]) {
                      i++;
                      //System.out.println("Currently Same for Character number "+i);
                      if (i >= match.length) {
                    	  //System.out.println("The EndTag is matched");
                    	  return true;
                      }
                    } else {
                    	i = 0;
                    }
                    // see if we've passed the stop point:
                    if (i == 0 && fsin.getPos() >= end) {
                    	//System.out.println("Already Passed the Stop Point");
                    	return false;
                    }
                  }
                }

        private boolean readUntilNeededTag() throws IOException {
            String tempString = "";
            boolean intag=false;
            while (true) {
              int b = fsin.read();
              // end of file:
              char current = (char) b;
              //System.out.println("Read byte: "+Character.toString(current));
              if (b == -1) return false;
              
              // If start of a tag
              if (current == '<') {
                    intag = true;
                    //System.out.println("The Open Bracket is Read");
                    tempString = "";
              }
              else if(intag){
                      if(current >='a' && current <='z'){
                              //insert value to currentTag
                              tempString += Character.toString(current);
                              //System.out.println("Current String:" +tempString);
                      }
                      else{
                	  		intag = false;
                	  		//for every tag, compare currentTag to check whether it's a tag
                              //System.out.println("Enter intag is False. Compare to: "+tempString);
                              for(String nTag : listTag){
                                      if(tempString.equals(nTag)){
                                              currentTag = tempString;
                                              tempString = "<"+tempString+" ";
                                              //System.out.println("Write this to buffer:"+tempString);
                                              buffer.write(tempString.getBytes("utf-8"));
                                              return true;
                                              
                                      }
                              }
                    }
                }
                // see if we've passed the stop point:
                if (fsin.getPos() >= end) {
                	//System.out.println("Exit because already reach end of FileSplit");
                	return false;
                }
            }
        }
    } 
}