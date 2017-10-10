package Lab2;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
        
public class StockProducer {
    // Set the stream and topic to publish to.
    public static String topic;
            
    // Declare a new producer
    public static KafkaProducer<String, JsonNode> producer;
        
    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException, TimeoutException {
        // check command-line args
        if(args.length != 5) {
            System.err.println("usage: StockProducer <broker-socket> <input-file> <stock-symbol> <output-topic> <sleep-time>");
            System.err.println("eg: StockProducer localhost:9092 /user/user01/LAB2/orcl.csv orcl prices 1000");
            System.exit(1);
        }
        
        // initialize variables
        String brokerSocket = args[0];
        String inputFile = args[1];
        String stockSymbol = args[2];
        String outputTopic = args[3];
        long sleepTime = Long.parseLong(args[4]);
        
        
        // configure the producer
        configureProducer(brokerSocket);    
        // TODO create a buffered file reader for the input file      1 
        // TODO loop through all lines in input file        1
        // TODO filter out "bad" records       
        // TODO create an ObjectNode to store data in       
        // TODO parse out the fields from the line and create key-value pairs in ObjectNode       
        // TODO sleep the thread        
        // TODO close buffered reader        
        // TODO close producer
        
        try{
        	File fl = new File(inputFile);
    	    FileReader fr = new FileReader(fl);
    	    BufferedReader br = new BufferedReader(fr);
        	            
            String input;
            while((input=br.readLine())!=null){
            	
            	String[] det = input.toString().split(",");
            	if(!det[0].contains("Date")){
            		ObjectNode in = JsonNodeFactory.instance.objectNode();
            		in.put("timestamp", det[0]);
            		in.put("open", det[1]);
            		in.put("high", det[2]);
            		in.put("low", det[3]);
            		in.put("close" ,det[4]);
            		in.put("volume", det[5]);  
            		in.put("stockSymbol", stockSymbol);
            		ProducerRecord<String, JsonNode> rec = new ProducerRecord<String, JsonNode>(outputTopic,stockSymbol,in);
            		System.out.println(stockSymbol + " : " + in.toString()); 
            		producer.send(rec);
            		
            		Thread.sleep(sleepTime);
            	}
            }
            
            br.close();
            fr.close();
            producer.close();
        }
        catch(IOException io){
            io.printStackTrace();
        }
        
    }

    public static void configureProducer(String brokerSocket) {
        Properties props = new Properties();
        props.put("value.serializer", "org.apache.kafka.connect.json.JsonSerializer");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("bootstrap.servers", brokerSocket);
        producer = new KafkaProducer<String, JsonNode>(props);
    }
}
