/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.kryptag.normalizer;

import com.google.gson.Gson;
import com.kryptag.rabbitmqconnector.MessageClasses.CreditMessage;
import com.kryptag.rabbitmqconnector.MessageClasses.LoanResponse;
import com.kryptag.rabbitmqconnector.RMQConnection;
import com.kryptag.rabbitmqconnector.RMQConsumer;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;

/**
 *
 * @author florenthaxha
 */
public class Consumer extends RMQConsumer{
    
    public Consumer(ConcurrentLinkedQueue q, RMQConnection rmq) {
        super(q, rmq);
    }

    @Override
    public void run() {
        while(Thread.currentThread().isAlive()){
            doWork();
        }
    }
    
    private void doWork(){
        Gson g = new Gson();
        if (!this.getQueue().isEmpty()) {
            if(this.getQueue().peek().toString().startsWith("<")){
                LoanResponse loanResp = fromXML(this.getQueue().remove().toString());
                this.getRmq().sendMessage(g.toJson(loanResp));
            }else{
                this.getRmq().sendMessage(this.getQueue().remove().toString());
            }
        }
    }
    
    private LoanResponse fromXML(String msg) {
        LoanResponse loanResp = null;
        try {
            JAXBContext jaxb = JAXBContext.newInstance(CreditMessage.class);
            Unmarshaller unMarshaller = jaxb.createUnmarshaller();
            StringReader reader = new StringReader(msg);
            loanResp = (LoanResponse) unMarshaller.unmarshal(reader);
            
            return loanResp;
        } catch (JAXBException ex) {
            Logger.getLogger(Consumer.class.getName()).log(Level.SEVERE, null, ex);
        }
        return loanResp;
    }
    
}
