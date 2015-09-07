/*
 * Caroline Pessoa Caetano - 408247
 * Henrique Squinello      - 408352
 * Trabalho 2 - Sistemas Distribuídos
 */
package t2sd;

import java.io.*;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Carol pc
 */
public class SDThread extends Thread{
    
    //Thread maintenance variables
    private int sequenceNumber;
    private List<DatagramSocket> clientList;
    private final int pID;
    //Only one number generator for all threads
    private static Random r = new Random();
    private int timeToSleep;
    private String type;
    //Flow control variables
    private boolean wants;
    private static boolean[] criticalZones; 
    
    public SDThread(int pID, String type){
        this.pID = pID;
        this.timeToSleep = 3000;
        this.type = type;
    }
    
    //Initialize the thread with the given information
    public SDThread(int pID, List<DatagramSocket> clientList, boolean wantsIt,String type) {
        this.clientList = clientList;
        //Start the sequence number at different places for different threads
        this.sequenceNumber = r.nextInt(4);
        this.pID = pID;
        this.wants = wantsIt;
        criticalZones =  new boolean[clientList.size()];
        this.timeToSleep = 3000;
        this.type = type;
    }
    
    
    //Getter and Setter for the Critical Zone variables
    public synchronized void setCriticalZone(boolean value){
        criticalZones[this.pID] = value;
    }
    
    public synchronized boolean getCriticalZone(){
        return criticalZones[this.pID];
    }
    
    //Send a multicast message from the current socket who wants the resource
    public void send(String message) {

        //New event, increment sequence number
        sequenceNumber = sequenceNumber + 1;

        //This is done so we can achieve total ordering between processes,
        //appending their unique ID to the sequence number
        int mID = (sequenceNumber * 10) + pID;

        //Create the pair to be send in broadcast
        Pair<String, Integer> outMessage = new Pair<String, Integer>(message, mID);

        //Send the message to all threads
        for (int i = 0; i < clientList.size(); i++) {
            DatagramSocket s = clientList.get(i);
            try {
                s.send(new DatagramPacket(serialize(outMessage), serialize(outMessage).length,
                        s.getLocalAddress(), s.getLocalPort()));
            } catch (IOException e) {
                System.out.println("Log: Error sending message, from " + pID + " to " + i + "; Retrying");
                e.printStackTrace();
            }
        }
    }
    
    /*  Check if the current Thread is using or wants the resource if not
        send an ACK.
        If it is using put the puts the Thread in a list to send ACK when 
        it finishes
        If it wants the resource the lower mID between the current message and
        its own message gets the resource
        
    */
    private List<Pair<String, Integer>> receive(Pair message, List<Pair<String, Integer>> messageList) {

        //Message received, adjust clock the following way:
        //1. Append the process ID to the sequence number for a fair comparison (see send function)
        //2. Store the highest sequence number WITHOUT the process specific ID
        //This is because the tie has already been broken, no need for the pID
        sequenceNumber = 1 + Math.max(sequenceNumber * 10 + pID, ((int) message.getQnt()));
        sequenceNumber = sequenceNumber / 10;

        //Retrieve the ID of the thread who sent the message
        int tID = (Integer) message.getQnt() % 10;
        
        Pair<String, Integer> ackMessage = new Pair<String, Integer>("ACK", (Integer) message.getQnt());
        DatagramSocket s = clientList.get(tID);
        
         //Insert the message in the list and reorder it
        messageList.add(message);
        Collections.sort(messageList, mIDComparator);
        
        if(!this.wants && !getCriticalZone()){
            /*  Thread does not want the resource, neither it is using so 
                it sends an ACK
            */
            try {
                s.send(new DatagramPacket(serialize(ackMessage), serialize(ackMessage).length,
                        s.getLocalAddress(), s.getLocalPort()));
            } catch (IOException e) {
                System.out.println("Log: Error sending ACK, from thread " + pID + " to " + tID);
            }
        }
        else if( this.wants ){
            /*  The thread wants the resource, then it checks the clock
                If the message has a lower clock than its own message send an ACK
            */
            
            for (int i = 0; i < messageList.size(); i++) {
                if ( Integer.parseInt(messageList.get(i).getmID()) % 10 == pID) {
                    if ((Integer) message.getQnt() < Integer.parseInt(messageList.get(i).getmID())) {
                        /*  If the mID receives is lower then its own message 
                            send an ACK
                        */
                        try {
                            s.send(new DatagramPacket(serialize(ackMessage), serialize(ackMessage).length,
                                    s.getLocalAddress(), s.getLocalPort()));
                        } catch (IOException e) {
                            System.out.println("Log: Error sending ACK, from thread " + pID + " to " + tID);
                        }
                        break;
                    }else{
                        //Not sure
                    }
                }
            }
            
        }else{
            /*  The thread is in the critical zone, it's using the resource 
                the message goes to a list to receive an ACK 
            */
        
        }
        return messageList;
    }
    
    //Processes ack messages
    private List<Pair<String, Integer>> ack(Pair message, List<Pair<String, Integer>> ackList) {

        //Message received, adjust clock the same way as receive()
        sequenceNumber = 1 + Math.max(sequenceNumber * 10 + pID, ((int) message.getQnt()));
        sequenceNumber = sequenceNumber / 10;

        //Message IDs are stored as strings in the ackList
        String mID = message.getmID().toString();

        //Search for the mID in the ackList. If found,
        //increment its counter, otherwise add it to the list
        for (int i = 0; i < ackList.size(); i++) {
            if (ackList.get(i).getmID().equals(mID)) {
                Pair<String, Integer> ack = ackList.remove(i);
                ack.setQnt(ack.getQnt() + 1);
                ackList.add(ack);
                Collections.sort(ackList, ackComparator);
                
                if(ack.getQnt() == clientList.size() - 1){
                    /*It means this thread can use the resource
                      So create a new thread with the same ID to use the Resource
                      while the original thread keep running normally
                    */
                    SDThread auxT = new SDThread(this.pID,"aux");
                    auxT.start();
                }
                return ackList;
            }
        }

        //mID not found, add it to the list and reorder
        ackList.add(new Pair<String, Integer>(mID, 1));
        Collections.sort(ackList, ackComparator);
        return ackList;
    }
    
    public void useResource(){
        
        setCriticalZone(true);
        
        try {
            Thread.sleep(this.timeToSleep);
        } catch (InterruptedException ex) {
            Logger.getLogger(SDThread.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        setCriticalZone(false);
        this.wants = false;
  
    }
    
    public void run(){
        
        if (this.type.equals("main")){
            //Keep track of every message received
            List<Pair<String, Integer>> messageList = new ArrayList<Pair<String, Integer>>();

            //Keep track of each message's ack quantity
            List<Pair<String, Integer>> ackList = new ArrayList<Pair<String, Integer>>();


            //Variables to keep track of the thread progress
            int messagesReceived = 0;
            int totalMessagesToSend = 3;
            int messagesSent = 0;
            byte[] data = new byte[10000000];
            
            //Create the packet to be sent
            DatagramPacket packet = new DatagramPacket(data, 10000000);
            /*for(int i = 0 ; i < 100 ; i++){
                System.out.println("Hi");
            }*/
            
        }else if(this.type.equals("aux")){
            useResource();
            /*for(int i = 0 ; i < 100 ; i++){
                System.out.println("Hi2");
            }*/
        }
    }
    
    
    //Compare Messages for sorting
    public static Comparator<Pair<String, Integer>> mIDComparator = new Comparator<Pair<String, Integer>>() {
        public int compare(Pair<String, Integer> p1, Pair<String, Integer> p2) {
            Integer mID1 = p1.getQnt();
            Integer mID2 = p2.getQnt();

            //ascending order
            return mID1 - mID2;

        }
    };

    public static Comparator<Pair<String, Integer>> ackComparator = new Comparator<Pair<String, Integer>>() {
        public int compare(Pair<String, Integer> p1, Pair<String, Integer> p2) {
            String m1 = p1.getmID().toUpperCase();
            String m2 = p2.getmID().toUpperCase();

            //ascending order
            return m1.compareTo(m2);

        }
    };

    public static byte[] serialize(Object obj) throws IOException {
        ByteArrayOutputStream b = new ByteArrayOutputStream();
        ObjectOutputStream o = new ObjectOutputStream(b);
        o.writeObject(obj);
        return b.toByteArray();
    }

    public static Object deserialize(byte[] bytes) throws IOException, ClassNotFoundException {
        ByteArrayInputStream b = new ByteArrayInputStream(bytes);
        ObjectInputStream o = new ObjectInputStream(b);
        return o.readObject();
    }
}
