package com.example.kafka;

import java.util.ArrayList; 
import java.util.Properties; 
import org.apache.kafka.clients.producer.KafkaProducer; 
import org.apache.kafka.clients.producer.ProducerConfig; 
import org.apache.kafka.clients.producer.ProducerRecord; 
import org.apache.kafka.common.serialization.StringSerializer; 


public class KafkaProducerDemo { 


    private static final String TOPIC_NAME = "ryantopic"; 
//    private static final String TOPIC_NAME = "TempTopic"; 


    public static void main (String [] args) { 


        KafkaProducer<String,String> producer=null; 

    
        try { 


            String bootstrapServers="127.0.0.1:9092"; 


            //create Producer properties 

            Properties properties=new Properties (); 

            properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers); 

            properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName ()); 

            properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); 



            //create the producer 

            producer =new KafkaProducer<String, String>(properties); 

            
            ArrayList<String> tempList=getTempList(); 

            
            for (String temp: tempList) 

            { 

                //create a producer record to send the data to Kafka 

                ProducerRecord<String,String> producerRecord=new ProducerRecord<String,String>(TOPIC_NAME,temp); 


                //send data -asynchrous 



                //using producer.send() we can send the data to the Kaffka  

                producer.send(producerRecord); 


                System.out.println("Succesfully sent temp data:"+temp+" to the Topic"); 

                
                Thread.sleep(4000); 


            } 


        } catch (Exception e) { 
            e.printStackTrace(); 
        } 


        finally 
        { 
            if (producer != null) 
            { 
                producer.flush(); 
                producer.close(); 	 
            } 
        }	 


    } 

  


    private static ArrayList<String> getTempList() { 

        ArrayList<String> tempList=new ArrayList<String>(); 

        tempList.add("78.C"); 

        tempList.add("84.C"); 

        tempList.add("94.C"); 

        tempList.add("74.C"); 

        tempList.add("89.C"); 

        return tempList; 

    } 



} 