package com.example.kafka;

import java.lang.reflect.Array; 
import java.time.Duration; 
import java.util.Arrays; 
import java.util.Properties; 
import org.apache.kafka.clients.consumer.ConsumerConfig; 
import org.apache.kafka.clients.consumer.ConsumerRecord; 
import org.apache.kafka.clients.consumer.ConsumerRecords; 
import org.apache.kafka.clients.consumer.KafkaConsumer; 
import org.apache.kafka.common.errors.WakeupException; 
import org.apache.kafka.common.serialization.StringDeserializer; 
  

public class KafkaConsumerDemo { 



    private static final String TOPIC_NAME = "ryantopic"; 
//    private static final String TOPIC_NAME = "TempTopic"; 

    

    public static void main(String[] args) { 

 

        String bootstrapServers ="127.0.0.1:9092";// kafka server ip address and port number 

        String group_id="group1"; 


        //create consumer config 

        Properties properties =new Properties(); 



        //creating consumer config 

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers); 

        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName()); 

        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName()); 

        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,group_id ); 

        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest"); 

 

        //create Kafka Consumer 

        final KafkaConsumer<String,String> consumer=new KafkaConsumer<String,String>(properties); 



        //get a reference to the current thread 
        
        final Thread mainThread =Thread.currentThread(); 

        

        //adding the shutdown hook 

        Runtime.getRuntime().addShutdownHook(new Thread() 

        { 

            public void run() 

            { 

                System.out.println("Detection  a shutdown,lets exit by calling consumer.wakeup()"); 

                consumer.wakeup(); 

        
                //join the main thread to allow the exceution of the  

                //code in the main thread 

                try { 

                    mainThread.join(); 

                } 

                catch(Exception e) 

                { 

                    e.printStackTrace(); 

                } 

            } 

        }); 


        try { 

            //subscriber consumer to our topics(s) 

            consumer.subscribe(Arrays.asList(TOPIC_NAME)); 

        

            //poll for new data 

            while(true) 

            { 

                ConsumerRecords<String,String> consumerRecords =consumer.poll(Duration.ofMillis(100)); 
            

                for(ConsumerRecord<String,String> consumerRecord:consumerRecords) 

                { 

                    System.out.println("Key: "+consumerRecord.key()+"Value:"+consumerRecord.value() ); 

                    System.out.println("Partition: "+consumerRecord.partition()+", Offset: "+consumerRecord.offset()); 

                } 

            } 
        

        }



        catch (WakeupException e) 

        { 

            System.out.println("Wake up exception"); 

            e.printStackTrace(); 

        } 



        catch(Exception e) 

        { 

            e.printStackTrace(); 

        } 



        finally { 

            consumer.close(); 

            System.out.println("the consumer is generally closed"); 

        } 

 

    } 

  

} 