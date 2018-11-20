/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.kryptag.normalizer;

import com.kryptag.rabbitmqconnector.Enums.ExchangeNames;
import com.kryptag.rabbitmqconnector.RMQConnection;
import com.kryptag.rabbitmqconnector.RMQConsumer;
import com.kryptag.rabbitmqconnector.RMQProducer;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 *
 * @author florenthaxha
 */
public class Main {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        RMQConnection rmqPub = new RMQConnection("guest", "guest", "datdb.cphbusiness.dk", 5672, ExchangeNames.NORMALIZER.name());
        RMQConnection rmqCon = new RMQConnection("guest", "guest", "datdb.cphbusiness.dk", 5672, ExchangeNames.NOMARLIZER_TOAGGREGATOR.name());
        ConcurrentLinkedQueue q = new ConcurrentLinkedQueue();
        RMQProducer rmqp = new RMQProducer(q, rmqPub);
        RMQConsumer rmqc = new RMQConsumer(q, rmqCon);
        rmqp.start();
        rmqc.start();
    }
    
}
