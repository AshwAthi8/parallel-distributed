import java.io.*;
import java.net.*;
import java.util.*;
import java.util.logging.*;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.*;
import java.net.*;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;

public class checkpoint {

	LinkedList<Integer> neighbours;
    int check,check1;
    int version,iterator;
    final int maxdelay = 1000;
	LinkedList<String> hostname;
    int[] label_value;
    static int totalnodes;
    LinkedList<String> port;
    LinkedList<Integer> nextHost;
    LinkedList<String> hostWork;
    LinkedList<String> lls,llr,vclock,itr;
    int[] last_label_rcvd, first_label_sent , vectorClock, vclockbackup,last_label_Sent;
    LinkedList<Integer> coverednodes;
    int[] last_checkpoint_rcvd,last_checkpoint_sent;
    static int id;
    static boolean sendappmessages,close;
    int taskid;
    String taskval;
    boolean task;


	public checkpoint(int id)
	{
		this.id = id;
        version = 0;
        iterator = 0;
        task = false;
        taskval= "e";
        taskid = -1;
        check = 0;
        check1 = 0;
        hostname = new LinkedList<>();
        sendappmessages = true;
        vclock = new LinkedList<>();
        itr = new LinkedList<>();
        llr = new LinkedList<>();
        lls = new LinkedList<>();
        coverednodes = new LinkedList<>();
        port = new LinkedList<>();
		neighbours = new LinkedList<>(); 
		nextHost = new LinkedList<>();
		hostWork = new LinkedList<>();
	}

	void startThread() {
        final int numProcesses = neighbours.size();
        final int nos = totalnodes;
        vectorClock = new int[nos]; 
        last_label_Sent = new int[numProcesses];
        last_label_rcvd = new int[numProcesses];
        label_value = new int[numProcesses];
        first_label_sent = new int[numProcesses];
        vclockbackup = new int[nos];
        last_checkpoint_rcvd = new int[numProcesses];
        last_checkpoint_sent = new int[numProcesses];
        new serverThread(this).start();
        new appThread(this).start();
        new appControlThread(this).start();
    }
    class serverThread extends Thread {

        checkpoint server;

        public serverThread(checkpoint main) {
            this.server = main;
            for(int i=0;i<last_label_rcvd.length;i++)
            {
            	last_label_rcvd[i] = -1;
            	first_label_sent[i] = -1;
            	last_checkpoint_rcvd[i] = -1;
                close = false;
                label_value[i] = 0;
            	last_checkpoint_sent[i] = -1;
            }

            for(int j=0;j<vectorClock.length;j++)
            {
                vectorClock[j]=0;
                vclockbackup[j]=0;
            }
        }

        public String sender(String msg, int id) {
            String sentence = msg;
            String modifiedSentence = "";
            
            try {
                BufferedReader inFromUser =
                    new BufferedReader(new InputStreamReader(System.in));
                 DatagramSocket clientSocket = new DatagramSocket();
                 InetAddress IPAddress = InetAddress.getByName(server.hostname.get(id - 1));
                 byte[] sendData = new byte[1024];
                 byte[] receiveData = new byte[1024];
                 sentence = msg;
                 sendData = sentence.getBytes();
                 DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, IPAddress, Integer.parseInt(server.port.get(id - 1)));
                 clientSocket.send(sendPacket);
                 clientSocket.close();
                 
            } catch (IOException e) {

            }
            return modifiedSentence;
        }

        public void updateVC(String vc)
        {
            String vcs[] = vc.split(",");
            for(int i=0;i<totalnodes;i++)
            {
                if(i==id)
                {
                    if(Integer.parseInt(vcs[i])>vectorClock[i])
                    {
                        vectorClock[i] = Integer.parseInt(vcs[i])+1;
                    }
                    else
                    {
                        vectorClock[i] +=1;
                    }
                }
                else
                {
                    vectorClock[i] = Integer.parseInt(vcs[i]);
                }
            }

        }

        public String covered()
        {
        	String cov = "";
        
            for(int i:coverednodes)
            {
                cov += i+",";
            }
        	return cov;
        }
        public boolean incovs(int x)
        {
            for(int i:coverednodes)
            {
                if(i==x)
                    return true;
            }
            return false;
        }

        public void addDone(String nodes)
        {
        	String allnodes[] = nodes.split(",");
        	for(int i=0;i<allnodes.length;i++)
        	{
                if(!incovs(Integer.parseInt(allnodes[i])))
        		  coverednodes.add(Integer.parseInt(allnodes[i]));
        	}
            if(!incovs(id+1))
                coverednodes.add(id+1);
        }

        public boolean inDone(int s)
        {
        	for(int i:coverednodes)
        	{
        		if(i==s)
        			return true;
        	}
        	return false;
        }
        public void takecheckpoint()
        {
            String l="";
            String s = "";
            String k = "";
            for(int j=0;j<vectorClock.length;j++)
            {
                k+=vectorClock[j]+",";
                vclockbackup[j] = vectorClock[j];
            }
            for(int i=0;i<last_label_rcvd.length;i++)
            {
                last_checkpoint_rcvd[i] = last_label_rcvd[i];
                l+=last_label_rcvd[i]+",";
                s+=first_label_sent[i]+",";
                last_checkpoint_sent[i] = first_label_sent[i];
                last_label_rcvd[i] = -1;
                first_label_sent[i] = -1; 
            }
            lls.add(s);
            llr.add(l);
            vclock.add(k);
            check =1;
            System.out.println("Taking checkpoint --  - - - - - --  -- - - - - - - -- ");

        }

        public void recover()
        {
            
            for(int i=0;i<last_label_rcvd.length;i++)
                last_label_rcvd[i] = -1;
            check =1;
            System.out.println("Before recovering");
            for(int j=0;j<vectorClock.length;j++)
            {
                System.out.println(j+1+" : "+vectorClock[j]);
                vectorClock[j] = vclockbackup[j];
            }

            System.out.println("Taking recovery --  - - - - - --  -- - - - - - - -- ");

            System.out.println("After recovering");
            for(int j=0;j<vectorClock.length;j++)
            {
                System.out.println(j+1+" : "+vectorClock[j]);
            }
        }


        public String floodNetwork(String message)
        {
            int x=0;
        	for(int s:neighbours)
        	{
        		if(!inDone(s))
        		{
        		
        			sender(message+";"+last_label_rcvd[x]+";"+iterator+";"+version,s);
        		}
                x++;
        	}
        	return "Test";
        }

        public String floodNetwork1(String message)
        {

            int x=0;
            for(int s:neighbours)
            {
                if(!inDone(s))
                {
                    sender(message+";"+last_label_Sent[x]+";"+iterator+";"+version,s);
                    System.out.println(message+";"+last_label_Sent[x]+";"+iterator+";"+version);
                }
                x++;
            }
            return "Test";
        }

        public void setLLR(int index,int val)
        {
            last_label_rcvd[index] = val;
        }

        public int getfLS(int x)
        {
            return first_label_sent[x];
        }
        public int getRealIndex(int x)
        {
            int t = 0;
            for(int i:neighbours)
            {
                if(i==x)
                {
                    return t;
                }
                t +=1;
            }
            return -1;
        }

        public String messageProcessor(DatagramPacket receivePacket, String message, DatagramSocket serverSocket) {
	        
            String[] messageparts = message.split(";");
            String from = messageparts[0];
            String type = messageparts[1];
            String vc = messageparts[2];
            String reply = "";


            if(Integer.parseInt(type)==Message.APP.id)
            {
                
                setLLR(getRealIndex(Integer.parseInt(from)+1),Integer.parseInt(messageparts[3]));
           
                updateVC(vc);
                reply = "OK";
            }

            else if(Integer.parseInt(type)==Message.CHECKPOINT.id)
            {
                System.out.print("vector clock for iteration "+iterator+" :");
                for(int i =0;i<vectorClock.length;i++)
                {
                    System.out.print(vectorClock[i]+" ");
                }
                System.out.println();
                sendappmessages = false;
                try{
                    appControlThread.sleep(100);
                    appThread.sleep(100);
                }
                catch(Exception e){}
                if(from.equals(""+id+""))
                {
                    while(coverednodes.size()>0)
                    {
                        coverednodes.remove(coverednodes.size()-1);
                    }
                    addDone(vc);
                    takecheckpoint();
                    floodNetwork(id+";"+1+";"+covered());
                }
                else
                {
                    while(coverednodes.size()>0)
                    {
                        coverednodes.remove(coverednodes.size()-1);
                    }

                    addDone(vc);
                System.out.println(Integer.parseInt(from)+1 + " "+getRealIndex(Integer.parseInt(from)+1)+" "+neighbours);
                if(getRealIndex(Integer.parseInt(from)+1) != -1)
                {
                if(Integer.parseInt(messageparts[3])>=getfLS(getRealIndex(Integer.parseInt(from)+1))&&getfLS(getRealIndex(Integer.parseInt(from)+1))>-1&&check==0)
                    takecheckpoint();
                coverednodes.add(-1);
                floodNetwork(id+";"+1+";"+covered());
                }
               
                try{
                    appControlThread.sleep(100);
                    appThread.sleep(100);
                }
                catch(Exception e){}}

                reply="checkpointinggg";
            }

            else if(Integer.parseInt(type)==Message.STEP.id)
            {
                
            }
            else if(Integer.parseInt(type)==Message.RECOVER.id)
            {
                try{
                    appControlThread.sleep(100);
                    appThread.sleep(100);
                }
                catch(Exception e){}
                if(from.equals(""+id+""))
                {
                    while(coverednodes.size()>0)
                    {
                        coverednodes.remove(coverednodes.size()-1);
                    }
                    addDone(vc);
                    recover();
                    floodNetwork1(id+";"+3+";"+covered());
                }
                if(!from.equals(""+id+""))
                {
                    while(coverednodes.size()>0)
                    {
                        coverednodes.remove(coverednodes.size()-1);
                    }
                    addDone(vc);
                    System.out.println("-----------------here--------------");
                    if(Integer.parseInt(messageparts[3])<last_label_rcvd[getRealIndex( Integer.parseInt(from)+1)]&&check==0)
                        recover();
                    floodNetwork1(id+";"+3+";"+covered());
                }
                
        
                reply="recover";
                
            }
            else if(Integer.parseInt(type)==Message.VER.id)
            {
               
                
            }
            else
            {
                System.out.println("Got this message: "+message);
                reply = "ok";
            }
            return reply;
        }

        @Override
        public void run() {
            String host = server.hostname.get(id);
            String clientSentence;
            int port = Integer.parseInt(server.port.get(id));
            String replymessage;
            try {
                String port2 = server.port.get(id);
                DatagramSocket serverSocket = new DatagramSocket(Integer.parseInt(port2));
                byte[] receiveData = new byte[1024];
                byte[] sendData = new byte[1024];
                while (close == false) {
                    System.out.println("Listening....");
                    DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
                    serverSocket.receive(receivePacket);
                    String sentence = "";
                    sentence = "";
                    sentence = new String( receivePacket.getData(),0, receivePacket.getLength());
                    
                    messageProcessor(receivePacket, sentence, serverSocket);
                    InetAddress IPAddress = receivePacket.getAddress();
                      int port1 = receivePacket.getPort();
                      sendData = "ok".getBytes();
                      DatagramPacket sendPacket =
                      new DatagramPacket(sendData, sendData.length, IPAddress, port1);
                      serverSocket.send(sendPacket);
                    
                }
            } catch (IOException e) {

            }
        }

    }
    class State {
        int n;
        public State() {}
        public State(int n) {
            this.n = n;
        }
    }

    enum Message {

        APP(0), CHECKPOINT(1), STEP(2), RECOVER(3), VER(4);

        final int id;

        Message(int id) {
            this.id = id;
        }

        int getMessageId() {
            return id;
        }
    }



    class appThread extends Thread
    {
        checkpoint server;
        public appThread(checkpoint main)
        {
            this.server = main;
        }

        public void setfLS(int index,int labelvalue)
        {
            if(first_label_sent[index]==-1)
                first_label_sent[index] = labelvalue; 

            last_label_Sent[index] = labelvalue;
        }

        public int getlabelvalue(int index)
        {
            label_value[index] +=1;
            return label_value[index];
        }

        public String vectorclock()
        {
            String vcs = "";
            vectorClock[id]+=1;
            for(int i=0;i<vectorClock.length;i++)
            {

                if(i!=vectorClock.length-1)
                    vcs += vectorClock[i]+",";
                else
                    vcs += vectorClock[i];
            }
            return vcs;
        }

        public void sendmessage()
        {
            int id1 = (int) (Math.random() * (neighbours.size()));
            
            int lbval = getlabelvalue(id1);
            String msg = id+";0;"+vectorclock()+";"+lbval;
            setfLS(id1,lbval);
            id1 = neighbours.get(id1);
            
            try {
                BufferedReader inFromUser =
                    new BufferedReader(new InputStreamReader(System.in));
                 DatagramSocket clientSocket = new DatagramSocket();
                 InetAddress IPAddress = InetAddress.getByName(server.hostname.get(id1 - 1));
                 byte[] sendData = new byte[1024];
                 byte[] receiveData = new byte[1024];
                 String sentence = msg;
                 sendData = sentence.getBytes();
                 DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, IPAddress, Integer.parseInt(server.port.get(id1 - 1)));
                 clientSocket.send(sendPacket);
                 DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
                 clientSocket.receive(receivePacket);
                 String modifiedSentence = new String(receivePacket.getData());
            
                 clientSocket.close();
            } catch (IOException e) {
                System.out.println(e);
            }   
        }

        @Override
        public void run() {
            
            while(iterator<nextHost.size())
            {
                if(true)
                {
                    try{
                    Thread.sleep(1000);

                    }
                    catch(Exception e)
                    {}
             
                    sendmessage();
                }
                else
                {
                   
                }
            }
            
        }
    }


    class appControlThread extends Thread
    {
        checkpoint server;
        int id;
        public appControlThread(checkpoint main)
        {
            this.server = main;
            this.id = main.id;
        }

        public void writetofile()
        {

        }

        @Override
        public void run() {
            int i = 0;
            
            while(iterator<nextHost.size())
            {
                try{
                    appControlThread.sleep(maxdelay);
                }
                catch(Exception e)
                {
                    System.out.println(e);
                }
                if(sendappmessages == false)
                {
                    
                    sendappmessages = true;
                    check = 0;
                    check1 = 0;
                    //System.out.println("Check:"+sendappmessages);
                    iterator +=1;
                    System.out.println("Iterator:"+iterator);
                    try{
                    appThread.sleep(maxdelay);
                    }
                    catch(Exception e)
                    {
                        System.out.println(e);
                    }
                }
                else
                {
                    try{
                    appControlThread.sleep(1000);}
                    
                catch(Exception e)
                {
                    System.out.println(e);
                }
                    sendappmessages = false;
                  
                    if(hostWork.size()>iterator)
                    {
                        int x = 0;
                        x = id+1;
                        if(nextHost.get(iterator)==id+1 && hostWork.get(iterator).equals("c"))
                        {

                            String msg = id+";1;"+"-1;100000;"+version+";"+iterator;
                            try {
                            BufferedReader inFromUser =
                                new BufferedReader(new InputStreamReader(System.in));
                             DatagramSocket clientSocket = new DatagramSocket();
                             InetAddress IPAddress = InetAddress.getByName(server.hostname.get(id));
                             byte[] sendData = new byte[1024];
                             byte[] receiveData = new byte[1024];
                             String sentence = msg;
                             sendData = sentence.getBytes();
                             try{
                                    appControlThread.sleep(500);
                                }
                                catch(Exception e)
                                {
                                    System.out.println(e);
                                }
                             DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, IPAddress, Integer.parseInt(server.port.get(id)));
                             clientSocket.send(sendPacket);
                             try{
                                    appControlThread.sleep(500);
                                }
                                catch(Exception e)
                                {
                                    System.out.println(e);
                                }
                             DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
                             clientSocket.receive(receivePacket);
                             String modifiedSentence = new String(receivePacket.getData());
                             clientSocket.close();
                            } catch (IOException e) {
                                System.out.println(e);
                            }  

                        }

                        if(nextHost.get(iterator)==id+1 && hostWork.get(iterator).equals("r"))
                        {
                            String msg = id+";3;"+"-1;100000;"+version+";"+iterator;
                            try {
                            BufferedReader inFromUser =
                                new BufferedReader(new InputStreamReader(System.in));
                             DatagramSocket clientSocket = new DatagramSocket();
                             InetAddress IPAddress = InetAddress.getByName(server.hostname.get(id));
                             byte[] sendData = new byte[1024];
                             byte[] receiveData = new byte[1024];
                             String sentence = msg;
                             sendData = sentence.getBytes();
                             try{
                                    appControlThread.sleep(500);
                                }
                                catch(Exception e)
                                {
                                    System.out.println(e);
                                }
                             DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, IPAddress, Integer.parseInt(server.port.get(id)));
                             clientSocket.send(sendPacket);
                             System.out.println("Taking 2");
                             try{
                                    appControlThread.sleep(500);
                                }
                                catch(Exception e)
                                {
                                    System.out.println(e);
                                }
                             DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
                             clientSocket.receive(receivePacket);
                             String modifiedSentence = new String(receivePacket.getData());
                             clientSocket.close();
                             System.out.println("Taking 3");
                        } catch (IOException e) {
                            System.out.println(e);
                        } 


                        
                        }
                        else
                        {
                            System.out.println("no no no");
                        }
                        
                    }
                }
                i++;
            }
            for(int j=0;j<vclock.size();j++)
            {
                System.out.println(vclock.get(j));
            }
             
            
        }
    }

public static void main(String args[]) {
        File f = new File("config.txt");
        id = Integer.parseInt(args[0]);
        checkpoint main = new checkpoint(id);
        int totalneigbhours = 0 ;
       
        totalnodes = 4;
            main.hostname.add("localhost");
            main.port.add("4567");
            main.neighbours.add(1);
            main.neighbours.add(3);
            main.neighbours.add(4);

            main.nextHost.add(1);
            main.hostWork.add("c");

            main.nextHost.add(2);
            main.hostWork.add("c");

            main.nextHost.add(3);
            main.hostWork.add("c");

            main.nextHost.add(4);
            main.hostWork.add("r");

            main.nextHost.add(2);
            main.hostWork.add("r");

            
            if(main.nextHost.get(0)==id)
            {
                main.task = true;
                main.taskid = id;
                main.taskval = main.hostWork.get(0); 
            }

            main.startThread();


        } 
   
}