package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;

import edu.buffalo.cse.cse486586.simpledynamo.Message.MessageType;

import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.os.AsyncTask;
import android.util.Log;
import android.widget.TextView;

public class ServerTask extends AsyncTask<ServerSocket, String, Void> {

	static final String TAG = ServerTask.class.getSimpleName();
	private static final String KEY_FIELD = "key";
	private static final String VALUE_FIELD = "value";
	private Context context;
	TextView textView;

	public static String predecessorPort;
	public static String predecessorHashCode;
	public static String successorPort;
	public static String successorHashCode;
	public static String myPort;
	public static String myHashCode;

	public static Cursor matrixCursor = null;
	public volatile static int recoveryCount = -1;

	static volatile Object queryLock = new Object();
	static volatile Object updateLock = new Object();
	static volatile Object recLock = new Object();

	public ServerTask(Context _context, String _myPort) throws NoSuchAlgorithmException {
		context = _context;
		myPort = _myPort;
		myHashCode = Utility.genHash(String.valueOf((Integer.parseInt(_myPort)) / 2));
		predecessorPort = Utility.getPredecessorPort(myPort);
		predecessorHashCode = Utility.genHash(String.valueOf((Integer.parseInt(predecessorPort)) / 2));;
		successorPort = Utility.getSuccessorPort(myPort);
		successorHashCode = Utility.genHash(String.valueOf((Integer.parseInt(successorPort)) / 2));;
	}

	@Override
	protected Void doInBackground(ServerSocket... sockets) {
		ServerSocket serverSocket = sockets[0];
		Message msgReceived = null;
		Socket socket = null;
		Cursor c = null;
		ObjectInputStream inputStream = null;

		recovery();

		try {
			while (true) 
			{
				socket = serverSocket.accept();
				inputStream = new ObjectInputStream(new BufferedInputStream(socket.getInputStream()));
				msgReceived = (Message) inputStream.readObject();

				MessageType msgType = msgReceived.getMsgType();
				Log.d(msgType.toString(), myPort + " - msg receiver");

				ContentValues cv = null;
				Message msg = null;

				switch (msgType) {

				case INSERT:	
					/*while(recoveryCount != -1) {
						Log.v("Insert waiting at ST", "for recovery to complete " + msgReceived.getKey() + " - Port:" + 
								msgReceived.getRequestingPort());
					}*/
					synchronized(updateLock)
					{
						SimpleDynamoProvider.replication.put(msgReceived.getKey(), true);

						cv = new ContentValues();
						cv.put(KEY_FIELD, msgReceived.getKey());
						cv.put(VALUE_FIELD, msgReceived.getValue());
						context.getContentResolver().insert(Utility.getUri(), cv);						
					}
					break;

				case QUERY: // simple query
					/*while(recoveryCount != -1) {
						Log.v("Query waiting at ST", "for recovery to complete " + msgReceived.getQueryParam() + " - Port:" + 
								msgReceived.getRequestingPort());
					}*/
					synchronized(queryLock)
					{
						c = context.getContentResolver().query(Utility.getUri(), null, 
								msgReceived.getQueryParam(), null, null);

						Log.v("query4 at server", "Cursor not NULL. Row count: " + c.getCount());

						// send reply to requester
						msg = new Message();
						msg.setMsgType(MessageType.QUERY_REPONSE);
						msg.setForwardingPort(msgReceived.getRequestingPort()); 
						msg.setRequestingPort(msgReceived.getRequestingPort());
						msg.setQueryParam(msgReceived.getQueryParam());

						try {
							int keyIndex = c.getColumnIndex(KEY_FIELD);
							int valueIndex = c.getColumnIndex(VALUE_FIELD);
							if (keyIndex == -1 || valueIndex == -1) {
								Log.v(TAG, "Query - Wrong columns");
								c.close();
								throw new Exception();
							}
							c.moveToFirst();
							msg.setKey(c.getString(keyIndex));
							msg.setValue(c.getString(valueIndex));
							c.close();

						} catch (Exception e) {
							e.printStackTrace();
						}	
						Utility.sendRequestToServer(msg);						
					}															
					break;

				case QUERY_REPONSE: // simple query						
					synchronized(queryLock)
					{										
						matrixCursor = new MatrixCursor(new String[] {
								SimpleDynamoProvider.KEY_FIELD,
								SimpleDynamoProvider.VALUE_FIELD });

						((MatrixCursor) matrixCursor).addRow(new String[] {
								msgReceived.getKey(), msgReceived.getValue() });
						Log.v("QUERY RESPONSE at Server", "Matrix rowcount: " + matrixCursor.getCount());

						SimpleDynamoProvider.waiting.remove(msgReceived.getQueryParam());
					}								
					break;

				case QUERY_GLOBAL:
					synchronized(queryLock)
					{
						SimpleDynamoProvider.isFirst = false;
						SimpleDynamoProvider.isWaiting = false;
						c = context.getContentResolver().query(Utility.getUri(), null, 
								msgReceived.getQueryParam(), null, null);

						// send reply to requester
						msg = new Message();
						msg.setMsgType(MessageType.QUERY_GLOBAL_RESPONSE);
						msg.setForwardingPort(msgReceived.getRequestingPort()); 
						msg.setRequestingPort(msgReceived.getRequestingPort());
						msg.setQueryParam(msgReceived.getQueryParam());
						Log.v("GLOBAL query", "msg received at Server at "+myPort+" - Row count: " + c.getCount());

						HashMap<String, String> allEntries = new HashMap<String, String>();					
						try {
							int keyIndex = c.getColumnIndex(KEY_FIELD);
							int valueIndex = c.getColumnIndex(VALUE_FIELD);
							if (keyIndex == -1 || valueIndex == -1) {
								Log.v(TAG, "Query - Wrong columns");
								c.close();
								throw new Exception();
							}
							c.moveToFirst();
							do{
								allEntries.put(c.getString(keyIndex), c.getString(valueIndex));
							}while(c.moveToNext());
							c.close();

						} catch (Exception e) {
							e.printStackTrace();
						}
						msg.setAllEntries(allEntries);
						Utility.sendRequestToServer(msg);
						Log.v("GLOBAL RESPONSE sent", "from " + myPort + " - Row count: " + c.getCount());

						SimpleDynamoProvider.isWaiting = false;
						SimpleDynamoProvider.isFirst = true;
					}
					break;

				case QUERY_GLOBAL_RESPONSE: 
					if(SimpleDynamoProvider.responseCount != -1) // not waiting anymore
						synchronized(queryLock)
						{
							Log.v("GLOBAL RESPONSE received", "Response count: "+SimpleDynamoProvider.responseCount);
							HashMap<String, String> allRows = msgReceived.getAllEntries();

							if(matrixCursor == null)
							{
								matrixCursor = new MatrixCursor(new String[] {
										SimpleDynamoProvider.KEY_FIELD,
										SimpleDynamoProvider.VALUE_FIELD });
							}

							for (String key : allRows.keySet()) {
								((MatrixCursor) matrixCursor).addRow(new String[] {
										key, allRows.get(key) });
							}
							SimpleDynamoProvider.responseCount++;

							if(SimpleDynamoProvider.responseCount == 3) // response received from all ports
							{
								SimpleDynamoProvider.isWaiting = false;
								SimpleDynamoProvider.isFirst = true;
							}
						}
					break;

				case DELETE:
					synchronized(updateLock)
					{
						SimpleDynamoProvider.replicated = true;										
						context.getContentResolver().delete(Utility.getUri(), msgReceived.getQueryParam(), null);

						SimpleDynamoProvider.replicated = false;
					}
					break;

				case RECOVERY:
					Cursor cur = context.getContentResolver().query(Utility.getUri(), null, "@", null, null);
					msg = new Message();
					msg.setMsgType(MessageType.RECOVERY_ACK);
					msg.setForwardingPort(msgReceived.getRequestingPort());
					msg.setRequestingPort(myPort); 
					Log.v("sending RECOVERY_ACK", myPort+" - Row count: " + cur.getCount());

					HashMap<String, String> allEntries = new HashMap<String, String>();										
					if(cur.getCount() > 0)
					{
						try {
							int keyIndex = cur.getColumnIndex(KEY_FIELD);
							int valueIndex = cur.getColumnIndex(VALUE_FIELD);
							if (keyIndex == -1 || valueIndex == -1) {
								Log.v(TAG, "Query - Wrong columns");
								cur.close();
								throw new Exception();
							}
							cur.moveToFirst();
							do{
								allEntries.put(cur.getString(keyIndex), cur.getString(valueIndex));
							}while(cur.moveToNext());
							cur.close();

						} catch (Exception e) {
							e.printStackTrace();
						}
					}					
					msg.setAllEntries(allEntries);
					Utility.sendRequestToServer(msg);
					break;

				case RECOVERY_ACK:										
					if(recoveryCount != -1) // recovery done/not required
					{
						Log.v("RECOVERY ACK", "Size: "+msgReceived.getAllEntries().size());
						if(msgReceived.getAllEntries().size() > 0)
						{							
							HashMap<String, String> recoveredList = msgReceived.getAllEntries();
							String prePort1 = Utility.getPredecessorPort(myPort);
							String prePort2 = Utility.getPredecessorPort(prePort1);

							for(String key: recoveredList.keySet())
							{
								Log.v("RECOVERING - INSERTING", key);

								String keyPort = Utility.getMappedPort(myPort, key);								
								if(keyPort.equals(myPort) || keyPort.equals(prePort1) || keyPort.equals(prePort2))
								{
									SimpleDynamoProvider.replication.put(key, true);									
									cv = new ContentValues();
									cv.put(KEY_FIELD, key);
									cv.put(VALUE_FIELD, recoveredList.get(key));
									context.getContentResolver().insert(Utility.getUri(), cv);	
								}	
							}
						}
						synchronized (recLock) 
						{
							recoveryCount++;
							if(recoveryCount == 3)
								recoveryCount = -1;	
							Log.v("RECOVERING - INSERTED", "received from "+msgReceived.getRequestingPort());												
						}
					}
					break;	
					
				case LOCAL_RECOVERY:
					Cursor recoveryCursor = context.getContentResolver().query(
							Utility.getUri(), null,
							msgReceived.getQueryParam(), null, null); // queryParameter
																		// = #
					msg = new Message();
					msg.setMsgType(MessageType.LOCAL_RECOVERY_ACK);
					msg.setForwardingPort(msgReceived.getRequestingPort());
					msg.setRequestingPort(myPort);
					Log.v("Sending LOCAL_RECOVERY_ACK", myPort
							+ " - Row count: " + recoveryCursor.getCount());

					HashMap<String, String> recoveredEntries = new HashMap<String, String>();
					if (recoveryCursor.getCount() > 0) {
						try {
							int keyIndex = recoveryCursor
									.getColumnIndex(KEY_FIELD);
							int valueIndex = recoveryCursor
									.getColumnIndex(VALUE_FIELD);
							if (keyIndex == -1 || valueIndex == -1) {
								Log.v(TAG, "Query - Wrong columns");
								recoveryCursor.close();
								throw new Exception();
							}
							recoveryCursor.moveToFirst();
							do {
								recoveredEntries.put(
										recoveryCursor.getString(keyIndex),
										recoveryCursor.getString(valueIndex));
							} while (recoveryCursor.moveToNext());
							recoveryCursor.close();

						} catch (Exception e) {
							e.printStackTrace();
						}
					}
					msg.setAllEntries(recoveredEntries);
					Utility.sendRequestToServer(msg);
					break;

				case LOCAL_RECOVERY_ACK:
					if (SimpleDynamoProvider.failureHandling != -1) {
						Log.v("LOCAL RECOVERY ACK", "Size: "
								+ msgReceived.getAllEntries().size());
						if (msgReceived.getAllEntries().size() > 0) {
							HashMap<String, String> recoveredList = msgReceived
									.getAllEntries();
							String prePort1 = Utility
									.getPredecessorPort(myPort);
							String prePort2 = Utility
									.getPredecessorPort(prePort1);

							for (String key : recoveredList.keySet()) {
								Log.v("LOCAL RECOVERY - INSERTING", key);

								String keyPort = Utility.getMappedPort(myPort,
										key);
								if (keyPort.equals(myPort)
										|| keyPort.equals(prePort1)
										|| keyPort.equals(prePort2)) {
									SimpleDynamoProvider.replication.put(key,
											true);
									cv = new ContentValues();
									cv.put(KEY_FIELD, key);
									cv.put(VALUE_FIELD, recoveredList.get(key));
									context.getContentResolver().insert(
											Utility.getUri(), cv);
								}
							}
						}
						synchronized (recLock) 
						{
							SimpleDynamoProvider.failureHandling++;
							if (SimpleDynamoProvider.failureHandling == 3)
								SimpleDynamoProvider.failureHandling = -1;
							Log.v("LOCAL RECOVERY - INSERTED", "received from "
									+ msgReceived.getRequestingPort());
						}
					}

				default:
					break;
				}

				inputStream.close();
				socket.close();				

			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		return null;
	}

	private void recovery() 
	{		
		SimpleDynamoProvider.replicated = true;										
		context.getContentResolver().delete(Utility.getUri(), "@", null);

		// forward request to all other ports
		Message m = new Message();
		m.setMsgType(MessageType.RECOVERY);
		m.setRequestingPort(myPort);

		// send message to all other ports
		int failed = 0;
		recoveryCount = 0;

		for(int i = 0; i < Utility.NODE_LIST.length; i++)
		{
			if(!Utility.NODE_LIST[i].equals(myPort)/* && !Utility.NODE_LIST[i].equals(ServerTask.successorPort)*/)
			{
				m.setForwardingPort(Utility.NODE_LIST[i]);
				failed += Utility.sendRequestToServer(m);
				if(failed > 1)
				{
					recoveryCount = -1;
					break;
				}
				Log.d("RECOVERY on " + myPort, "fwding to all ports - " + Utility.NODE_LIST[i]);	
			}
		}
		Log.d("RECOVERY on " + myPort, "fwded to all ports - RecCount: " + recoveryCount);	
	}
}