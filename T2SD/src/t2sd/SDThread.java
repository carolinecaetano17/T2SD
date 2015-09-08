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

public class SDThread extends Thread {

    //Thread maintenance variables
    private int sequenceNumber;
    private List<DatagramSocket> clientList;
    private final int pID;
    private final int TOTAL_ORDER_MULTIPLIER = 10;

    //Only one number generator for all threads
    private static Random r = new Random();
    private int timeToSleep;

    //Flow control variables
    private boolean wants;
    private static boolean[] criticalZones;

    //Initialize the thread with the given information
    public SDThread(int pID, List<DatagramSocket> clientList, boolean wantsIt) {
        this.clientList = clientList;
        //Start the sequence number at different places for different threads
        this.sequenceNumber = r.nextInt(4);
        this.timeToSleep = r.nextInt(2000);
        this.pID = pID;
        this.wants = wantsIt;
        criticalZones = new boolean[clientList.size()];
    }

    //Send a multicast message from the current socket that wants the resource
    public void send(String message, List<Pair<String, Integer>> messageList) {

        //New event, increment sequence number
        sequenceNumber = sequenceNumber + 1;

        //This is done so we can achieve total ordering between processes,
        //appending their unique ID to the sequence number
        int mID = (sequenceNumber * TOTAL_ORDER_MULTIPLIER) + pID;

        //Create the pair to be sent in broadcast
        Pair<String, Integer> outMessage = new Pair<String, Integer>(message, mID);

        //Store own message in the list and reorder it (to keep track of ACK messages)
        if (!message.equals("ACK")) {
            messageList.add(outMessage);
            Collections.sort(messageList, mIDComparator);
        }

        //Send the message to all threads
        for (int i = 0; i < clientList.size(); i++) {
            if (i != this.pID) {
                DatagramSocket s = clientList.get(i);
                try {
                    s.send(new DatagramPacket(serialize(outMessage), serialize(outMessage).length,
                            s.getLocalAddress(), s.getLocalPort()));
                } catch (IOException e) {
                    System.out.println("Log: Error sending message, from " + pID + " to " + i + ".");
                    e.printStackTrace();
                }
            }
        }
    }

    /*  Check if the current Thread is using or wants the resource. If not,
        send an ACK.
        If it is using puts the Thread in a list to send ACK when
        it finishes
        If it wants the resource the lower mID between the current message and
        its own message gets the resource
        
    */
    private List<Pair<String, Integer>> receive(Pair message, List<Pair<String, Integer>> messageList) {

        //Message received, adjust clock the following way:
        //1. Append the process ID to the sequence number for a fair comparison (see send function)
        //2. Store the highest sequence number WITHOUT the process specific ID
        //This is because the tie has already been broken, no need for the pID
        sequenceNumber = 1 + Math.max(sequenceNumber * TOTAL_ORDER_MULTIPLIER + pID, ((int) message.getmID()));
        sequenceNumber = sequenceNumber / TOTAL_ORDER_MULTIPLIER;

        //Retrieve the ID of the thread who sent the message
        int tID = (Integer) message.getmID() % TOTAL_ORDER_MULTIPLIER;

        int mID = (sequenceNumber * TOTAL_ORDER_MULTIPLIER) + pID;

        Pair<String, Integer> ackMessage = new Pair<String, Integer>("ACK", mID);
        DatagramSocket s = clientList.get(tID);

        if (!this.wants && !getCriticalZone()) {
            /*  Thread does not want the resource, neither is it using so
                it sends an ACK
            */
            try {
                s.send(new DatagramPacket(serialize(ackMessage), serialize(ackMessage).length,
                        s.getLocalAddress(), s.getLocalPort()));
            } catch (IOException e) {
                System.out.println("Log: Error sending ACK, from thread " + pID + " to " + tID);
            }
        } else if (this.wants) {
            /*  The thread wants the resource, then it checks the clock
                If the message has a lower clock than its own message send an ACK
            */
            for (int i = 0; i < messageList.size(); i++) {
                if (messageList.get(i).getmID() % TOTAL_ORDER_MULTIPLIER == pID) {
                    if ((Integer) message.getmID() < (Integer) messageList.get(i).getmID()) {
                        /*  If the mID received is lower than its own message
                            send an ACK
                        */
                        try {
                            s.send(new DatagramPacket(serialize(ackMessage), serialize(ackMessage).length,
                                    s.getLocalAddress(), s.getLocalPort()));
                        } catch (IOException e) {
                            System.out.println("Log: Error sending ACK, from thread " + pID + " to " + tID);
                        }
                    } else {
                        /* Thread wants the resource and has a lower logical clock.
                           We insert the message into the list of messages that want to use the
                           resource. When we are done, we send an ack to the first message in this list
                        */

                        //Insert the message in the list and reorder it
                        messageList.add(message);
                        Collections.sort(messageList, mIDComparator);
                    }
                }
            }

        } else {
            /*  The thread is in the critical zone, it's using the resource.
                The message goes to a list to receive an ACK. Same as above.
            */
            //Insert the message in the list and reorder it
            messageList.add(message);
            Collections.sort(messageList, mIDComparator);

        }
        return messageList;
    }

    //Processes ack messages
    private Pair<String, Integer> ack(Pair message, Pair<String, Integer> threadAck) {

        //Message received, adjust clock the same way as receive()
        sequenceNumber = 1 + Math.max(sequenceNumber * TOTAL_ORDER_MULTIPLIER + pID, ((int) message.getmID()));
        sequenceNumber = sequenceNumber / TOTAL_ORDER_MULTIPLIER;

        if (threadAck != null) {
            threadAck.setmID(threadAck.getmID() + 1);
            if (threadAck.getmID() == clientList.size() - 1) {
                /*It means this thread can use the resource
                  So create a new thread with the same ID to use the Resource
                  while the original thread keep running normally
                */
                System.out.println("Thread " + pID + " preparing to enter critical zone");
                setCriticalZone(true);
                System.out.println("Thread " + this.pID + " in critical zone");
                try {
                    this.sleep(this.timeToSleep);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                System.out.println("Thread " + this.pID + " leaving critical zone after " + this.timeToSleep + "ms");
                setCriticalZone(false);
                this.wants = false;
                //Thread has accessed critical zone. Counter goes back to 0.
                return new Pair<String, Integer>("" + this.pID, 0);
            }
            return threadAck;
        } else {
            //This is the first ACK this thread has received of its own request for critical zone
            return new Pair<String, Integer>("" + this.pID, 1);
        }
    }

    public void useResource() {

        setCriticalZone(true);

        try {
            System.out.println("Thread " + this.pID + " in critical zone");
            Thread.sleep(this.timeToSleep);
            System.out.println("Thread " + this.pID + " leaving critical zone after " + this.timeToSleep + "ms");
        } catch (InterruptedException ex) {
            Logger.getLogger(SDThread.class.getName()).log(Level.SEVERE, null, ex);
        }

        setCriticalZone(false);

    }

    public void run() {

        //Keep track of every message received
        List<Pair<String, Integer>> messageList = new ArrayList<Pair<String, Integer>>();

        //Keep track of own message's ack quantity
        Pair<String, Integer> threadAcks = null;

        //Variables to keep track of the thread progress
        int messagesReceived = 0;
        int totalMessagesToSend = 1;
        int messagesSent = 0;
        byte[] data = new byte[10000000];

        //Create the packet to be used to receive data
        DatagramPacket packet = new DatagramPacket(data, 10000000);
        
        //Name of the file that will be used as Resource
        String fileName = "output.txt";
        
        while (true) {

            /*  If we have already sent and received every message we are done
                Rationale: clientList - 1 ACKs per message +
                clientList messages received from each thread (including itself)
                In this implementation we simulate that all the Threads wants 
                the Resource.
            */        
            if (messagesReceived == (totalMessagesToSend * (clientList.size() - 1)) +
                    totalMessagesToSend * clientList.size()) {
                clientList.get(pID).close();
                return;
            }

            //Send a message and update the quantity of messages sent by this thread
            if (messagesSent < totalMessagesToSend) {
                send(fileName, messageList);
                messagesSent++;
            }

            //Listen to own socket for data
            try {
                clientList.get(pID).receive(packet);
                if (packet.getLength() != 0) {
                    try {
                        Pair<String, Integer> m = (Pair<String, Integer>) deserialize(packet.getData());

                        //If it is an ACK
                        if (m.getMessage().equals("ACK")) {
                            threadAcks = ack(m, threadAcks);
                        } else {
                            //If it is a USE message
                            messageList = receive(m, messageList);
                        }

                        //This will only happen when the thread has used the critical zone
                        if (threadAcks != null && threadAcks.getmID() == 0) {
                            if (messageList != null) {
                                //Send ACKs to every message in the queue
                                for (Pair<String, Integer> mes : messageList) {
                                    //Retrieve the ID of the thread who sent the message
                                    int tID = mes.getmID() % TOTAL_ORDER_MULTIPLIER;

                                    int mID = (sequenceNumber * TOTAL_ORDER_MULTIPLIER) + pID;

                                    Pair<String, Integer> ackMessage = new Pair<String, Integer>("ACK", mID);
                                    DatagramSocket s = clientList.get(tID);
                                    try {
                                        s.send(new DatagramPacket(serialize(ackMessage), serialize(ackMessage).length,
                                                s.getLocalAddress(), s.getLocalPort()));
                                    } catch (IOException e) {
                                        System.out.println("Log: Error sending ACK, from thread " + pID + " to " + tID);
                                    }
                                }
                            }
                        }

                        //Keep track of how many messages are left
                        messagesReceived++;

                    } catch (Exception e) {
                        System.out.println("Log: stream header corrupted");
                        e.printStackTrace();
                    }
                }
            } catch (IOException e) {
                System.out.println("Log: timeout on thread " + pID);
            }
        }
    }


    //Compare Messages for sorting
    public static Comparator<Pair<String, Integer>> mIDComparator = new Comparator<Pair<String, Integer>>() {
        public int compare(Pair<String, Integer> p1, Pair<String, Integer> p2) {
            Integer mID1 = p1.getmID();
            Integer mID2 = p2.getmID();

            //ascending order
            return mID1 - mID2;

        }
    };

    public byte[] serialize(Object obj) throws IOException {
        ByteArrayOutputStream b = new ByteArrayOutputStream();
        ObjectOutputStream o = new ObjectOutputStream(b);
        o.writeObject(obj);
        return b.toByteArray();
    }

    public Object deserialize(byte[] bytes) throws IOException, ClassNotFoundException {
        ByteArrayInputStream b = new ByteArrayInputStream(bytes);
        ObjectInputStream o = new ObjectInputStream(b);
        return o.readObject();
    }

    //Getter and Setter for the Critical Zone variables
    public synchronized void setCriticalZone(boolean value) {
        criticalZones[this.pID] = value;
    }

    public synchronized boolean getCriticalZone() {
        return criticalZones[this.pID];
    }
}
