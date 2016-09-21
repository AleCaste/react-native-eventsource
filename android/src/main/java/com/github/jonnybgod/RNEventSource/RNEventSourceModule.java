package com.github.jonnybgod.RNEventSource;

import java.io.IOException;

import com.facebook.common.logging.FLog;
import com.facebook.react.bridge.Arguments;
import com.facebook.react.bridge.ReactApplicationContext;
import com.facebook.react.bridge.ReactContext;
import com.facebook.react.bridge.ReactContextBaseJavaModule;
import com.facebook.react.bridge.ReactMethod;
import com.facebook.react.bridge.WritableMap;
import com.facebook.react.common.ReactConstants;
import com.facebook.react.modules.core.DeviceEventManagerModule;

import java.net.URI;
import java.net.URL;

// Documentation: https://kaazing.com/doc/5.0/amqp_client_docs/apidoc/client/java/amqp/client/overview-summary.html
import org.kaazing.net.sse.SseEventReader;
import org.kaazing.net.sse.SseEventSource;
import org.kaazing.net.sse.SseEventSourceFactory;
import org.kaazing.net.sse.SseEventType;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import android.util.Log;

//import com.fasterxml.jackson.core.JsonGenerationException;
//import com.fasterxml.jackson.databind.JsonMappingException;
//import com.fasterxml.jackson.databind.ObjectMapper;


public class RNEventSourceModule extends ReactContextBaseJavaModule {

  // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  // JSON support
  private static final String TAG = "RNEventSourceModule"; //"RNEventSourceModule";
  /*
  private ObjectMapper jsonm = new ObjectMapper();
  private String JACKSON_mapToStr_safe(Object json_map) {
    String returned_value = null;
    try {
      returned_value = jsonm.writeValueAsString(json_map);
    } catch (Exception e) {
      Log.e(TAG, e.toString(), e);
    }
    return returned_value;
  }
  */
  // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

  private Map<Integer, SseEventSource> mEventSourceConnections = new HashMap<>();
  private Map<Integer, Thread> mEventReaderThreads = new HashMap<>();

  private SseEventSourceFactory factory = SseEventSourceFactory.createEventSourceFactory();
  private ReactContext mReactContext;

  public RNEventSourceModule(ReactApplicationContext context) {
    super(context);
    mReactContext = context;
  }

  private void sendEvent(String eventName, WritableMap params) {
    mReactContext
      .getJSModule(DeviceEventManagerModule.RCTDeviceEventEmitter.class)
      .emit(eventName, params);
  }

  @Override
  public String getName() {
    return "RNEventSource";
  }

  @ReactMethod
  public void connect(final String url, final int id) {
    try {

      final SseEventSource source = factory.createEventSource(new URI(url));
      //final SseEventSource source = factory.createEventSource(URI.create(url));

      source.connect();

      mEventSourceConnections.put(id, source);
      WritableMap params = Arguments.createMap();
      params.putInt("id", id);
      sendEvent("eventsourceOpen", params);

      Thread sseEventReaderThread = new Thread() {
        public void run() {
          try {
            SseEventReader reader = source.getEventReader();
     
            SseEventType type = null;
            while ((type = reader.next()) != SseEventType.EOS) {
              switch (type) {
                case DATA:
                  String name;
                  String data;
                  try {
                    name = reader.getName();
//Log.i("Alex","  [RNEventSourceModule/connect] - data:"+JACKSON_mapToStr_safe(reader.getData()));                    
                    data = reader.getData().toString();
//Log.i("Alex","  [RNEventSourceModule/connect] - data 1:"+data);
                  } catch (IOException e) {
//Log.i("Alex","  [RNEventSourceModule/connect] - ERROR 1:"+e.getMessage());
                    notifyEventSourceFailed(id, e.getMessage());
                    return;
                  }

                  // Send event to React Native
                  WritableMap params = Arguments.createMap();
                  params.putInt("id", id);
                  params.putString("type", name!=null ? name : "message");
                  params.putString("data", data);
//Log.i("Alex","  [RNEventSourceModule/connect] - data 2:"+data);
//Log.i("Alex","  [RNEventSourceModule/connect] - params:"+params);                  
                  sendEvent("eventsourceEvent", params);
                  
                  break;
                case EMPTY:
                  break;
              }
            }
            
            notifyEventSourceFailed(id, "Connection with the event source was closed.");
            close(id);
          }
          catch (Exception e) {
//Log.i("Alex","  [RNEventSourceModule/connect] - ERROR 2:"+e.getMessage());            
            notifyEventSourceFailed(id, e.getMessage());

            Thread.currentThread().interrupt();
            mEventReaderThreads.remove(id);
            return;
          }
        }
      };

      sseEventReaderThread.start();
      mEventReaderThreads.put(id, sseEventReaderThread);
    }
    catch (Exception e) {
      notifyEventSourceFailed(id, e.getMessage());
    }
  }

  @ReactMethod
  public void close(int id) {
    SseEventSource source = mEventSourceConnections.get(id);
    Thread thead = mEventReaderThreads.get(id);
    if (source == null) {
      // EventSource is already closed
      // Don't do anything, mirror the behaviour on web
      FLog.w(
        ReactConstants.TAG,
        "Cannot close EventSource. Unknown EventSource id " + id);

      return;
    }
    try {
      thead.interrupt();
      source.close();
      mEventSourceConnections.remove(id);
      mEventReaderThreads.remove(id);
    } catch (Exception e) {
      FLog.e(
        ReactConstants.TAG,
        "Could not close EventSource connection for id " + id,
        e);
    }
  }

  private void notifyEventSourceFailed(int id, String message) {
    WritableMap params = Arguments.createMap();
    params.putInt("id", id);
    params.putString("message", message);
    sendEvent("eventsourceFailed", params);
  }
}