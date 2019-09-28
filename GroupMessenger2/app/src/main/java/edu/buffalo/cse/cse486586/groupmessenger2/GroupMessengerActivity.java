package edu.buffalo.cse.cse486586.groupmessenger2;

import android.app.Activity;
import android.content.ContentValues;
import android.content.Context;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.Bundle;
import android.telephony.TelephonyManager;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.view.View;
import android.widget.EditText;
import android.widget.TextView;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * GroupMessengerActivity is the main Activity for the assignment.
 *
 * @author stevko
 *
 */
public class GroupMessengerActivity extends Activity {

    static final String TAG = GroupMessengerActivity.class.getSimpleName();
    static final int SERVER_PORT = 10000;
    static int finalCount =0;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_group_messenger);

        /*
         * TODO: Use the TextView to display your messages. Though there is no grading component
         * on how you display the messages, if you implement it, it'll make your debugging easier.
         */
        TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());

        TelephonyManager tel = (TelephonyManager) this.getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        /*
         * Registers OnPTestClickListener for "button1" in the layout, which is the "PTest" button.
         * OnPTestClickListener demonstrates how to access a ContentProvider.
         */
        findViewById(R.id.button1).setOnClickListener(
                new OnPTestClickListener(tv, getContentResolver()));

        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket");
            return;
        }
        final EditText editText = (EditText) findViewById(R.id.editText1);


        View.OnClickListener TestOnClickListener = new View.OnClickListener() {
            @Override
            public void onClick(View v){

                String msg = editText.getText().toString() + "\n";
                editText.setText(""); // This is one way to reset the input box.
                //TextView localTextView = (TextView) findViewById(R.id.textView1);
                //localTextView.append("\t" + msg + "\n"); // This is one way to display a string.
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, myPort);

            }
        };

        findViewById(R.id.button4).setOnClickListener(TestOnClickListener);


        /*
         * TODO: You need to register and implement an OnClickListener for the "Send" button.
         * In your implementation you need to get the message from the input box (EditText)
         * and send it to other AVDs.
         */
    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.activity_group_messenger, menu);
        return true;
    }




    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        //Data Structure to maintain all messages
        private class Custom implements Comparable<Custom>{
            String msg;
            String status;
            Float value;
            long timestamp;

            public Custom(String msg,String status,Float value,long timestamp ){
                this.msg=msg;
                this.status=status;
                this.value=value;
                this.timestamp=timestamp;

            }
            public int compareTo(Custom compareValue){
                return this.value.compareTo(compareValue.value);}
        }



        int count=0;
        int max=0;
        private Uri buildUri(String scheme, String authority) {
            Uri.Builder uriBuilder = new Uri.Builder();
            uriBuilder.authority(authority);
            uriBuilder.scheme(scheme);
            return uriBuilder.build();
        }
        private final Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.groupmessenger2.provider");
        ContentValues[] cv = new ContentValues[10];

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];

            List<Custom> cs = new ArrayList<Custom>();

            String[] ans = new String[1];
                while(true) {
                    Socket clientsocket;
                    PrintWriter out = null;
                    try {
                        serverSocket.setSoTimeout(2000);
                        clientsocket = serverSocket.accept();
                        clientsocket.setSoTimeout(2000);


                    BufferedReader in = new BufferedReader(
                            new InputStreamReader(clientsocket.getInputStream()));

                    ans[0] = in.readLine();

                        out = new PrintWriter(clientsocket.getOutputStream(),true );


                    }catch (NullPointerException n){
                        //Log.e(TAG, "Null Server  failed");
                        ans[0]="0";
                    }catch (SocketTimeoutException s){
                        //Log.e(TAG, "Socket Server failed");
                        ans[0]="0";
                    }
                    catch (IOException e) {
                        e.printStackTrace();
                    }

                    //When the actual message is being sent to the server
                    if(ans[0].contains("actual message"))
                    {
                        String str;
                        count++;
                        max++;
                        str=(count>=max)?count+"."+ans[0].charAt(0):(max)+"."+ans[0].charAt(0);
                        Custom custom = new Custom(ans[0].substring(ans[0].indexOf(":")+1),"Undecided",Float.parseFloat(str),System.currentTimeMillis());
                        cs.add(custom);

                        out.println(str);


                    }

                    //When the decided sequence number is sent to the server
                    if(ans[0].contains("decided")) {
                        String theMsg = ans[0].substring(ans[0].indexOf(":") + 1);
                        Float theVal = Float.parseFloat(ans[0].substring(0, ans[0].indexOf(".") + 2));


                        for (int i = 0; i < cs.size(); i++) {
                            if (cs.get(i).msg.equals(theMsg)) {
                                cs.get(i).value = theVal;
                                cs.get(i).status = "Decided";
                                break;
                            }
                        }
                    }
                        Collections.sort(cs);
                        if(cs.size()>0)
                        max=Math.round(cs.get(cs.size()-1).value);


                            for (int i = 0; i < cs.size(); i++) {
                                if (cs.get(i).status.equals("Undecided")) {
                                    if (System.currentTimeMillis() >= cs.get(i).timestamp + 4000) {
                                        cs.get(i).status = "Delivered";
                                        publishProgress(cs.get(i).msg);
                                    }
                                }
                            }


                        int j=0;
                        while(cs.size()>0&&cs.get(j).status.equalsIgnoreCase("Delivered")){
                            if(j==cs.size()-1)
                                break;
                            j++;

                        }
                        while(cs.size()>0&&cs.get(j).status.equalsIgnoreCase("Decided"))
                        {
                            cs.get(j).status="Delivered";
                            publishProgress(cs.get(j).msg);
                            j++;
                            if(j>cs.size()-1)
                                break;

                        }

                }

        }

        protected void onProgressUpdate(String...strings) {
            /*
             * The following code displays what is received in doInBackground().
             */
            String strReceived = strings[0].trim();
            TextView remoteTextView = (TextView) findViewById(R.id.textView1);
            remoteTextView.append( "\n"+strReceived);

            cv[0] = new ContentValues();
            cv[0].put("key",String.valueOf(finalCount++));
            cv[0].put("value",strReceived);
            getContentResolver().insert(mUri,cv[0]);
            return;
        }
    }

    private class ClientTask extends AsyncTask<String, Void, Void> {
        float[] count = new float[5];


        @Override
        protected Void doInBackground(String... msgs) {
                String[] remotePort = new String[]{"11108", "11112", "11116", "11120", "11124"};

                for(int i=0;i<5;i++) {
                    try {
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(remotePort[i]));

                        socket.setSoTimeout(2000);

                        String msgToSend = msgs[0];
                        PrintWriter out =
                                new PrintWriter(socket.getOutputStream(), true);

                        out.println(String.valueOf(i) + "This is the actual message:" + msgToSend);

                        BufferedReader in = new BufferedReader(
                                new InputStreamReader(socket.getInputStream()));
                        count[i] = Float.parseFloat(in.readLine());

                        socket.close();
                    }catch (NullPointerException n){
                        Log.e(TAG, "Null Pointer failed");

                    }catch (SocketTimeoutException s){
                        Log.e(TAG, "Timeout failed");

                    }catch (IOException e) {
                    Log.e(TAG, "ClientTask socket IOException");
                    }
                }
                float max = count[0];
                for(int j=0;j<count.length;j++)
                {
                    if(count[j]>max)
                        max=count[j];
                }

                try {
                for(int i=4;i>=0;i--){
                    Socket socket = null;

                        socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(remotePort[i]));

                    PrintWriter out =
                            new PrintWriter(socket.getOutputStream(), true);
                    out.println(max+"The decided number:"+msgs[0]);
                    socket.close();

                }
            } catch (IOException e) {
                e.printStackTrace();
            }

            return null;
        }
    }

}