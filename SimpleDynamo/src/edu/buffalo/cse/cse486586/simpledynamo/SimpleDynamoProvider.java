package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.IOException;
import java.net.ServerSocket;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;

import edu.buffalo.cse.cse486586.simpledynamo.Message.MessageType;

import android.content.ContentProvider;
import android.content.ContentUris;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {

	static final String TAG = SimpleDynamoProvider.class.getSimpleName();
	String myPort;

	SQLiteDatabase myDatabase;
	DhtHelper sqlHelper;
	Context context;
	static final String DB_NAME = "simpleDynamoDB";
	static final int DB_VERSION = 1;

	static final String TABLE_NAME = "dynamoTable2";
	static final String KEY_FIELD = "key";
	static final String VALUE_FIELD = "value";
	static final String CREATE_TABLE = "CREATE TABLE IF NOT EXISTS " + TABLE_NAME + " ( "
			+ KEY_FIELD + " TEXT UNIQUE NOT NULL, " + VALUE_FIELD + " TEXT NOT NULL );";

	// for global query
	public volatile static boolean isFirst = true;
	public volatile static boolean isWaiting = false;
	public volatile static int responseCount = -1;
	
	// for simple query
	public static HashMap<String, Boolean> waiting = new HashMap<String, Boolean>();
	
	// for insert
	public static HashMap<String, Boolean> replication = new HashMap<String, Boolean>();	
	// for delete
	public volatile static boolean replicated = false;
	
	public volatile static int failureHandling = -1;
	
	static volatile Object queryLock = new Object();
	static volatile Object updateLock = new Object();
	
	@Override
	public String getType(Uri uri) {
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) 
	{
		Log.v("inserting CP1", values.toString());		
		synchronized(updateLock)
	    {
			Log.v("inserting CP2", values.toString());		
			String mappedPort = Utility.getMappedPort(myPort, (String) values.get(KEY_FIELD));

			Message m = new Message();	
			m.setRequestingPort(myPort);
			m.setKey((String) values.get(KEY_FIELD));
			m.setValue((String) values.get(VALUE_FIELD));
			
			//if(replicated || mappedPort.equals(myPort))
			if(replication.containsKey(m.getKey()) || mappedPort.equals(myPort))
			{
				Log.v("inserted in "+myPort, values.toString());
				long rowId = myDatabase.insertWithOnConflict(TABLE_NAME, null,
						values, SQLiteDatabase.CONFLICT_REPLACE);
				
				if (rowId > 0) 
					uri = ContentUris.withAppendedId(uri, rowId);
				
				if(replication.containsKey(m.getKey()))
				{
					replication.remove(m.getKey());
					return uri;
				}
			}
			else
			{
				// send insert request to coordinator
				m.setMsgType(MessageType.INSERT);
				m.setForwardingPort(mappedPort);
				
				Log.d("INSERT", "Incorrect Node "+ myPort + ", sending msg to " + mappedPort);
				Utility.sendRequestToServer(m); // forward to server
			}			

			//  first time request - send message to next 2 successors
			if(!replication.containsKey(m.getKey())) //
			{
				String port1 = Utility.getSuccessorPort(mappedPort);
				String port2 = Utility.getSuccessorPort(port1);
				
				Log.v("Msg1 for replication to " + port1, values.toString());			
				m.setMsgType(MessageType.INSERT);			
				m.setForwardingPort(port1);
				Utility.sendRequestToServer(m); 	
					
				Log.v("Msg2 for replication to " + port2, values.toString());			
				m.setMsgType(MessageType.INSERT);			
				m.setForwardingPort(port2);
				Utility.sendRequestToServer(m); 
			}
			replication.remove(m.getKey());
			return uri;			
	    } // end synchronized		 			    
	}

	@Override
	public boolean onCreate() {
		context = getContext();

		// Calculate the port number that this AVD listens on
		TelephonyManager tel = (TelephonyManager) context
				.getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(
				tel.getLine1Number().length() - 4);
		myPort = String.valueOf((Integer.parseInt(portStr) * 2));

		// keep port open for listening to requests
		try {
			ServerSocket serverSocket = new ServerSocket(Integer.parseInt(Utility.SERVER_PORT));
			new ServerTask(context, myPort).executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);

		} catch (IOException e) {
			e.printStackTrace();
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}

		sqlHelper = new DhtHelper(context);
		myDatabase = sqlHelper.getWritableDatabase();
		if (myDatabase == null)
			return false;
		else
			return true;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) 
	{	
		Cursor cursor;				
		if (selection.equals("@")) // retrieve local entries
		{
			cursor = myDatabase.rawQuery("SELECT * from " + TABLE_NAME,
					selectionArgs);
			cursor.setNotificationUri(context.getContentResolver(), uri);
			Log.v("queryLOCAL", selection);
			return cursor;				
		} 
		else if (selection.equals("*")) // retrieve global entries
		{
			cursor = myDatabase.rawQuery("SELECT * from " + TABLE_NAME,
						selectionArgs);
			Log.v("queryGLOBAL", selection + " - Row count: " + cursor.getCount());
			
			// return if only one node
			if (myPort.equals(ServerTask.successorPort)) 
				return cursor;

			if (isFirst) 
			{					
				isWaiting = true;
				responseCount = 0; // waiting for responses
			}
			else // return result immediately if not querying port
			{
				return cursor;
			}
			Log.v("Global Query on " + myPort, "isFirst: " + isFirst + " isWaiting: " + isWaiting);

			// forward request to all other ports
			Message m = new Message();
			m.setMsgType(MessageType.QUERY_GLOBAL);
			m.setRequestingPort(myPort);
			m.setQueryParam(selection);				

			ServerTask.matrixCursor = null;
			// send message to all other ports
			for(int i = 0; i < Utility.NODE_LIST.length; i++)
			{
				if(!Utility.NODE_LIST[i].equals(myPort)/* && !Utility.NODE_LIST[i].equals(ServerTask.successorPort)*/)
				{
					m.setForwardingPort(Utility.NODE_LIST[i]);
					Utility.sendRequestToServer(m);
					Log.d("QUERY GLOBAL on " + myPort, selection + " - "
							+ "fwding to all ports - " + Utility.NODE_LIST[i]);	
				}
			}

			while (isWaiting) {
				try {
					Thread.sleep(4);
					//Log.d("QUERY GloBAL", myPort + " waiting for all  entries");
					Log.d("QUERY GloBAL waiting",  "No of rows: " + (ServerTask.matrixCursor == null ? 0 : ServerTask.matrixCursor.getCount()));
				} catch (InterruptedException e) {
					e.printStackTrace();
				}					
			}
			isWaiting = false; // reset
			isFirst = true;
			responseCount = -1; // not waiting anymore			

			return ServerTask.matrixCursor;			
		} 
		else // simple query
		{	
			cursor = myDatabase.rawQuery("SELECT * from " + TABLE_NAME
						+ " WHERE " + KEY_FIELD + " = ?", new String[] { selection });
			Log.v("query1: "+ selection, "Cursor1 row-count: "+cursor.getCount());
				
			if(cursor.getCount() >= 1)	
			{
				return cursor;
			}
			
			String mappedPort = Utility.getMappedPort(myPort, selection);					
			if (mappedPort.equals(myPort) /*|| Utility.getSuccessorPort(mappedPort).equals(myPort)*/) 
			{
				do
				{					
					cursor = myDatabase.rawQuery("SELECT * from " + TABLE_NAME
							+ " WHERE " + KEY_FIELD + " = ?", new String[] { selection });
					Log.v("query02: "+selection, "Cursor2 row-count: "+cursor.getCount());
						
					if(cursor.getCount() >= 1)					
						return cursor;
					
					try {
						Thread.sleep(10);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					
					Log.v("query12: "+selection, "Cursor2 row-count: "+cursor.getCount());
					
				} while(cursor.getCount() < 1);					
			} 
			else // forward request to mappedPort
			{				
				synchronized(queryLock)
				{	
					if(!waiting.containsKey(selection))
						waiting.put(selection, true);
					ServerTask.matrixCursor = null;
					
					// forward request to mappedPort
					Message m = new Message();
					m.setMsgType(MessageType.QUERY);
					m.setForwardingPort(mappedPort); 
					m.setRequestingPort(myPort);
					m.setQueryParam(selection);
					Log.v("query3: " + selection, "Key not found at "+ myPort + 
							", sending msg to " + mappedPort);					
					Utility.sendRequestToServer(m);
				
					while(waiting.containsKey(selection)) {}  // wait if waiting(key) = true
					waiting.remove(selection);        // remove key after wait ends
										
					Log.v("QUERY RESPONSE RECVD", selection + " - " + ServerTask.matrixCursor);
					return ServerTask.matrixCursor;
					
				}// end synchronized
			} 								
		}							
		return cursor;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		return 0;
	}
	
	public class DhtHelper extends SQLiteOpenHelper {

		public DhtHelper(Context context) {
			super(context, DB_NAME, null, DB_VERSION);
		}

		@Override
		public void onCreate(SQLiteDatabase db) {
			db.execSQL(CREATE_TABLE);
		}

		@Override
		public void onUpgrade(SQLiteDatabase db, int oldVer, int newVer) {
			db.execSQL("DROP TABLE IF EXISTS " + TABLE_NAME);
			onCreate(db);
		}
	}

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) 
	{
		Log.v("delete", selection);
		int rows = 0;
		//synchronized(updateLock)
		{	
			if (selection.equals("@")) {
				rows = myDatabase.delete(TABLE_NAME, null, null);
				
			} else if (selection.equals("*")) 
			{
				rows = myDatabase.delete(TABLE_NAME, null, null);
				
				if (myPort.equals(ServerTask.successorPort)) 
					return rows;
				
			} else // simple delete
			{							  
				Log.v("deleted", selection);
				Message m = new Message(); 
				m.setRequestingPort(myPort);
				m.setQueryParam(selection);
				
				String mappedPort = Utility.getMappedPort(myPort, selection);
				if (replicated || mappedPort.equals(myPort)) 
				{
					rows = myDatabase.delete(TABLE_NAME, KEY_FIELD + "= ?", new String[] { selection });
					
					if(replicated)
					{
						replicated = false;
						return rows;
					}
				} 
				else // forward request to mappedPort
				{
					m.setMsgType(MessageType.DELETE);	
					m.setForwardingPort(mappedPort);
					Log.d("DELETE", selection + " - Key not found at "+ myPort + 
							", sending msg to " + mappedPort);
					Utility.sendRequestToServer(m);
				}
				
				// first time request - send message to next 2 successors				
				if(!replicated) 
				{
					String port1 = Utility.getSuccessorPort(mappedPort);
					String port2 = Utility.getSuccessorPort(port1);
					
					Log.v("Msg1 for replica delete " + port1, selection);			
					m.setMsgType(MessageType.DELETE);			
					m.setForwardingPort(port1);					
					Utility.sendRequestToServer(m); 	
						
					Log.v("Msg2 for replica delete " + port2, selection);			
					m.setMsgType(MessageType.DELETE);			
					m.setForwardingPort(port2);
					Utility.sendRequestToServer(m); 
				}						
				replicated = false; // reset
				
				Log.d("DELETE", selection + " - " + myPort + "Entries deleted.");					
			}				
		}
		return rows;	
	}

}
