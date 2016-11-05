/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package jp.takida.mqtt;

import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;

/**
 *
 * @author fabrizio
 */
public class SendToken implements MqttCallback {
    
    private MqttClient client;
    
    private MqttConnectOptions options;
    
    private long threadId;

    private String token;
    
    private long id;

    public SendToken() {
    }
    
    
    private void connectMQTT() {
        try {
            //MqttClient client = new MqttClient("tcp://localhost:1883", "pahomqttpublish19"  + threadId);
            client = new MqttClient(Param.address, "sink" + threadId);
            
            //client = new MqttClient("tcp://localhost:1883", "pahomqttpublish" + threadId);
            client.setCallback(this);

            options = new MqttConnectOptions();
            options.setUserName("sink");
            options.setPassword("password".toCharArray());
            //options.setWill("pahodemo/test", "crashed".getBytes(), 2, true);

            client.connect(options);

        } catch (MqttException e) {
            e.printStackTrace();
        }
    }
    
   public void sendToken(){
   
        if (client == null || !client.isConnected()) {
            this.connectMQTT();
        }
        try {
            //client.publish("impress/demo0", token.getBytes(), 0, false);
             client.publish("impress/demo0", token.getBytes(), 1, true);
        } catch (MqttException e) {
            //e.printStackTrace();
            Logger.getLogger(SendToken.class.getName()).log(Level.SEVERE, null, e);
        }
       
       
   }

    @Override
    public void connectionLost(Throwable cause) {
        //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        connectMQTT();
        sendToken();
        
    }

    @Override
    public void messageArrived(String topic, MqttMessage message) throws Exception {
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {
    }

    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }
    
    
    
}
