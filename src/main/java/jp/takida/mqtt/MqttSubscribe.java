package jp.takida.mqtt;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.Objects;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;

public class MqttSubscribe implements MqttCallback {

    public MqttSubscribe() {
    }

    public static void main(String[] args) {
        new MqttSubscribe().subscribe();
    }
    public static int cont = 0;
    static String[] ids = new String[100000];

    public void subscribe() {
        try {
            //MqttClient client = new MqttClient("tcp://localhost:61613", "pahomqttpublish1");
            MqttClient client = new MqttClient(Param.address, "subibe");
            client.setCallback(this);

            MqttConnectOptions options = new MqttConnectOptions();
            options.setUserName("adm");
            options.setPassword("pass".toCharArray());
             
            client.connect(options);

            client.subscribe(Param.topic);
            try {
                System.in.read();
            } catch (IOException e) {
                // If we can't read we'll just exit
            }

            client.disconnect();
        } catch (MqttException e) {
            e.printStackTrace();
            System.out.println("erro: " + e.getMessage().toString());
        }
    }

    public void connectionLost(Throwable msg) {
        System.out.println("erro: " + msg.getMessage()+"  " + msg.getCause());
        msg.printStackTrace();
    }

    public void deliveryComplete(IMqttDeliveryToken arg0) {
        System.out.println("Delivery completed.");
    }

    public void messageArrived(String topic, MqttMessage message) throws Exception {
        //  String time = new Timestamp(System.currentTimeMillis()).toString();
        long time = (System.currentTimeMillis());
        System.out.println("Time:\t" + time
                + "  Topic:\t" + topic
                + "  Message:\t" + new String(message.getPayload())
                + "  QoS:\t" + message.getQos() + " cont: "+ cont);
        String m = new String(message.getPayload());
        if (!"FINISH".equals(m) && !"START".equals(m)) {
            cont ++;
            writeFile(m, time);
        }

    }

//UID=1;type=traffic;id=100068;message=72.89475,43.425323,16.28621,35.467022,75.55648,76.651764,78.75302,52.171783,27.653915,61.379642,3.7755003,67.0252,46.11978,43.396996,46.076477,62.13194,36.7783,78.21247,82.160355,18.115602,;P1=1474392066363;P2=1474392066370;P3=1474392067383;P4=1474392067421;P5=?;P6=?;P7=?;REP=1;EXP=1
    public void writeFile(String m, long time) throws IOException {
        //boolean erro = false; 
        String[] msg = m.split(";");
        String[] uid = msg[0].split("=");  
        String[] type = msg[1].split("=");
        String[] id = msg[2].split("=");
//        for(int i =0; i<= cont;i++){
//            if(uid[1].equals(ids[i])){
//                System.out.println("ERRO - ID REPETIDO "+ uid[1]);
//                erro = true;
//            }
//           // System.out.print(" id["+i+"]="+ids[1]+";");
//        }
      
//        if(!erro){
//            ids[cont] = uid[1];
//           //System.out.println(" ID OK num: "+ uid[1]);
//        }
        String[] p = new String[8];
        String[] temp = new String[2];
        for (int i = 0; i < 8; i++) {
            temp = msg[i + 4].split("=");
            p[i] = temp[1];
            //System.out.print("P"+(i+1)+" = "+ p[i]+ "  ");
        }
        //System.out.println();
        p[7] = String.valueOf(time);
        String[] rep = msg[12].split("=");
        String[] exp = msg[13].split("=");
       // Param.path += exp[1]; 
        File arquivo = new File(Param.path +exp[1]+ ".csv");
        try (FileWriter fw = new FileWriter(arquivo, true); BufferedWriter bw = new BufferedWriter(fw)) {
            m = "\"" + uid[1] + "\"" + ";" + "\"" + exp[1] + "\"" + ";" + "\"" + rep[1] + "\"" + ";" + "\"" + type[1] + "\"" + ";" + "\"" + id[1] + "\"" + ";";
            for (int i = 0; i < 8; i++) {
                m +="\"" + p[i] + "\"" + ";";
            }
            // devNo;msgNo;time
            System.out.println(m);
            bw.write(m);
            bw.newLine();
        }
    }
}
