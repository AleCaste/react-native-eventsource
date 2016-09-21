/**
 * Copyright (c) 2007-2014 Kaazing Corporation. All rights reserved.
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.kaazing.net.sse.impl;

import org.kaazing.gateway.client.impl.http.HttpRequest;
import org.kaazing.gateway.client.impl.http.HttpRequest.Method;
import org.kaazing.gateway.client.impl.http.HttpRequestAuthenticationHandler;
import org.kaazing.gateway.client.impl.http.HttpRequestHandler;
import org.kaazing.gateway.client.impl.http.HttpRequestHandlerFactory;
import org.kaazing.gateway.client.impl.http.HttpRequestListener;
import org.kaazing.gateway.client.impl.http.HttpRequestRedirectHandler;
import org.kaazing.gateway.client.impl.http.HttpRequestTransportHandler;
import org.kaazing.gateway.client.impl.http.HttpResponse;
import org.kaazing.gateway.client.impl.ws.ReadyState;
import org.kaazing.gateway.client.util.HttpURI;
import org.kaazing.gateway.client.util.WrappedByteBuffer;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.Charset;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import android.util.Log;

/**
 * ServerSentEvent stream implementation.
 */
public class SseEventStream {
  
  // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  private static String java_escape_string(String s) {
    // NOTE: you may also use org.apache.commons.lang.StringEscapeUtils.escapeJava(s);
    // ... from Apache Commons Lang  (https://commons.apache.org/proper/commons-lang/download_lang.cgi)
    if (s==null)  return "null";
    StringBuilder sb = new StringBuilder();
    char ch = 'a';
    Integer ls = null; try { ls = s.length(); } catch (Exception e) { ls = 0; }
    for (int i=0; i<ls; i++) {
      try {
        ch = s.charAt(i);
        switch (ch){
          case '\b': sb.append("\\b"); break;             /* \u0008: backspace (BS) */
          case '\t': sb.append("\\t"); break;             /* \u0009: horizontal tab (HT) */
          case '\n': sb.append("\\n"); break;             /* \u000a: linefeed (LF) */
          case '\f': sb.append("\\f"); break;             /* \u000c: form feed (FF) */
          case '\r': sb.append("\\r"); break;             /* \u000d: carriage return (CR) */
          case '\"': sb.append("\\\""); break;             /* \u0022: double quote (") */
          case '\'': sb.append("\\'"); break;             /* \u0027: single quote (') */
          case '\\': sb.append("\\\\"); break;            /* \u005c: backslash (\) */      
          // ... rest of escape characters
          // \U{0000-FFFF}     Unicode [Basic Multilingual Plane only, see below] hex value 
          //                   does not handle unicode values higher than 0xFFFF (65535),
          //                   the high surrogate has to be separate: \uD852\uDF62
          //                   Four hex characters only (no variable width)
          // \{0-377}          \u0000 to \u00ff: from octal value 
          //                   1 to 3 octal digits (variable width)
          default: sb.append(ch);
        }
      } catch (Exception e) { }
    }
    return sb.toString();
  }  
  // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  
  
    private static final String  MESSAGE = "message";
    private static final String  CLASS_NAME = SseEventStream.class.getName();
    private static final Logger  LOG = Logger.getLogger(CLASS_NAME);
    private static final Charset UTF_8 = Charset.forName("UTF-8");
    private final StringBuffer dataBuffer = new StringBuffer();

    private transient static final Timer timer = new Timer("reconnect", true);

    static final HttpRequestHandlerFactory SSE_HANDLER_FACTORY = new HttpRequestHandlerFactory() {
        
        @Override
        public HttpRequestHandler createHandler() {
            HttpRequestAuthenticationHandler authHandler = new HttpRequestAuthenticationHandler();
            HttpRequestRedirectHandler redirectHandler = new HttpRequestRedirectHandler();
            HttpRequestHandler transportHandler = HttpRequestTransportHandler.DEFAULT_FACTORY.createHandler();
  
            authHandler.setNextHandler(redirectHandler);
            redirectHandler.setNextHandler(transportHandler);
            
            return authHandler;
        }
    };

    private ReadyState             readyState = ReadyState.CONNECTING;
    private String                 lastEventId = "";
    private boolean                aborted = false;
    private boolean                errored = false;
    private String                 sseLocation;
    private long                   retry = 3000; // same as actionscript implementation
    private boolean                immediateReconnect = false;
    private String                 messageBuffer = "";
    private HttpRequest            sseSource;
    private AtomicBoolean          progressEventReceived = new AtomicBoolean(false);
    private AtomicBoolean          reconnected = new AtomicBoolean(false);
    private HttpRequestHandler     sseHandler;
    private SseEventStreamListener listener;


    public SseEventStream(String sseLoc) throws IOException {
        LOG.entering(CLASS_NAME, "<init>", sseLoc);
        
        // Validate the URI.
        URI.create(sseLoc);
 
        this.sseLocation = sseLoc;

        sseHandler = SSE_HANDLER_FACTORY.createHandler();
        
        sseHandler.setListener(new EventStreamHttpRequestListener());
    }

    public ReadyState getReadyState() {
        return readyState;
    }

    public void stop() {
        LOG.entering(CLASS_NAME, "stop");
        readyState = ReadyState.CLOSED;
        sseHandler.processAbort(sseSource);
        aborted = true;
    }

    public void connect() throws IOException {
        LOG.entering(CLASS_NAME, "connect");
        if (lastEventId != null && (lastEventId.length() > 0)) {
            sseLocation += (sseLocation.indexOf("?") == -1 ? "?" : "&") + ".ka=" + lastEventId;
        }

        try {
            HttpURI uri = new HttpURI(this.sseLocation);
            sseSource = new HttpRequest(Method.GET, uri, true);
              // See: https://github.com/remy/polyfills/blob/master/EventSource.js
              sseSource.setHeader("Accept", "text/event-stream");  // VERY IMPORTANT FOR CERTAN SERVERS!!! Otherwise they might respond with 405 status!!
              sseSource.setHeader("Cache-Control", "no-cache");
              sseSource.setHeader("X-Requested-With", "XMLHttpRequest");
            sseHandler.processOpen(sseSource);

            if (!reconnected.get()) {
                TimerTask timerTask = new TimerTask() {
                    @Override
                    public void run() {
                        // TODO: Why is this commented out? - no fallback to long polling?
                        
                        // if (!SseEventStream.this.progressEventReceived.get() && readyState != ReadyState.CLOSED) {
                        // if (sseLocation.indexOf("?") == -1) {
                        // sseLocation += "?.ki=p";
                        // }
                        // else {
                        // sseLocation += "&.ki=p";
                        // }
                        // listener.reconnectScheduled = true;
                        // reconnected.set(true);
                        // retry = 0;
                        // try {
                        // connect();
                        // }
                        // catch (IOException e) {
                        // // TODO Auto-generated catch block
                        // e.printStackTrace();
                        // }
                        // }
                    }
                };
                Timer timer = new Timer();
                timer.schedule(timerTask, 3000);
            }
        } catch (Exception e) {
            LOG.log(Level.INFO, e.getMessage(), e);
            doError(e);
        }
    }
    
    public long getRetryTimeout() {
        return retry;
    }
    
    public void setRetryTimeout(long millis) {
        retry = millis;
    }

    private synchronized void reconnect() {
        LOG.entering(CLASS_NAME, "reconnect");
        if (readyState != ReadyState.CLOSED) {
            TimerTask task = new TimerTask() {
                @Override
                public void run() {
                    try {
                        connect();
                    } catch (IOException e) {
                        LOG.log(Level.INFO, e.getMessage(), e);
                        throw new RuntimeException(e);
                    }
                }
            };
            timer.schedule(task, retry);
        }
    }

    private synchronized void processProgressEvent(String message) {
        //android.util.Log.i(CLASS_NAME, "[processProgressEvent] - message:"+java_escape_string(message));
        
        String line;
        try {
            //android.util.Log.i(CLASS_NAME, "[processProgressEvent] -   messageBuffer 1:"+java_escape_string(messageBuffer));
            messageBuffer = messageBuffer + message;
            //android.util.Log.i(CLASS_NAME, "[processProgressEvent] -   messageBuffer 2:"+java_escape_string(messageBuffer));
            String field = null;
            String value = null;
            String name = MESSAGE;
            String data = "";
            immediateReconnect = false;
            while (!aborted && !errored) {
                line = fetchLineFromBuffer();
                //android.util.Log.i(CLASS_NAME, "[processProgressEvent] -   line:"+java_escape_string(line));
                if (line == null) {
                    break;
                }

                if (line.length() == 0 && dataBuffer.length() > 0) {
                    //android.util.Log.i(CLASS_NAME, "[processProgressEvent] -     dataBuffer (to be sent):"+java_escape_string(dataBuffer.toString()));
                    synchronized (dataBuffer) {
                        int dataBufferlength = dataBuffer.length();
                        if (dataBuffer.charAt(dataBufferlength - 1) == '\n') {
                            dataBuffer.replace(dataBufferlength - 1, dataBufferlength, "");
                        }
                        doMessage(name, dataBuffer.toString());
                        //android.util.Log.i(CLASS_NAME, "[processProgressEvent] -     dataBuffer (sent):"+java_escape_string(dataBuffer.toString()));
                        dataBuffer.setLength(0);
                    }
                }
                
                int colonAt = line.indexOf(':');
                if (colonAt == -1) {
                    // no colon, line is field name with empty value
                    field = line;
                    value = "";
                } else if (colonAt == 0) {
                    // leading colon indicates comment line
                    continue;
                } else {
                    field = line.substring(0, colonAt);
                    int valueAt = colonAt + 1;
                    if (line.length() > valueAt && line.charAt(valueAt) == ' ') {
                        valueAt++;
                    }
                    value = line.substring(valueAt);
                }
                //android.util.Log.i(CLASS_NAME, "[processProgressEvent] -     field:"+field);
                //android.util.Log.i(CLASS_NAME, "[processProgressEvent] -     value:"+java_escape_string(value));
                // process the field of completed event
                if (field.equals("event")) {
                    name = value;
                } else if (field.equals("id")) {
                    this.lastEventId = value;
                } else if (field.equals("retry")) {
                    retry = Integer.parseInt(value);
                } else if (field.equals("data")) {
                    // deliver event if data is specified and non-empty, or name is specified and not "message"
                    //android.util.Log.i(CLASS_NAME, "[processProgressEvent] -     Is data message!");
                    if (value != null || (name != null && name.length() > 0 && !MESSAGE.equals(name))) {
                        dataBuffer.append(value).append("\n");
                        //android.util.Log.i(CLASS_NAME, "[processProgressEvent] -     dataBuffer (so far):"+java_escape_string(dataBuffer.toString()));
                    }
                } else if (field.equals("location")) {
                    if (value != null && value.length() > 0) {
                        this.sseLocation = value;
                    }
                } else if (field.equals("reconnect")) {
                    immediateReconnect = true;
                }
            }

            if (immediateReconnect) {
                retry = 0;
                // this will be done on the load
            }
        } catch (Exception e) {
            LOG.log(Level.INFO, e.getMessage(), e);
            doError(e);
        }
    }

    private String fetchLineFromBuffer() {
      //android.util.Log.i(CLASS_NAME, "[fetchLineFromBuffer] - "+java_escape_string(this.messageBuffer.toString()));
      String ret = null;
      String s = this.messageBuffer;
      if (s==null || s=="")  return ret;
      StringBuilder sb = new StringBuilder();
      boolean line_started = false;
      int ls  = -1; try { ls = s.length(); } catch (Exception e) { ls = 0; }
      int ito = -1;
      char ch = 'a';
      go_through_string: for (int i=0; i<ls; i++) {
        try {
          ch = s.charAt(i);
          if (line_started==false) {
            if (Character.isWhitespace(ch)==false)  { line_started = true; sb.append(ch); }
          } else {
            switch (ch){
              case '\r': case '\n':  ito = i; break go_through_string;
              default: sb.append(ch);
            }
          }
        } catch (Exception e) { }
      }
      ret = sb.toString();
      if (ito!=-1)  {
        //sb = new StringBuilder();
        //for (i=ito+1; i<ls; i++)   sb.append( s.charAt(i) );
        //messageBuffer = sb.toString();
        this.messageBuffer = s.substring(ito + 1);
      } else this.messageBuffer = "";
      return ret;
    }

    private class EventStreamHttpRequestListener implements HttpRequestListener {
        private final String CLASS_NAME = EventStreamHttpRequestListener.class.getName();
        private final Logger LOG = Logger.getLogger(CLASS_NAME);

        boolean reconnectScheduled = false;

        EventStreamHttpRequestListener() {
            LOG.entering(CLASS_NAME, "<init>");
        }

        @Override
        public void requestReady(HttpRequest request) {
        }

        @Override
        public void requestOpened(HttpRequest request) {
            doOpen();
        }

        @Override
        public void requestProgressed(HttpRequest request, WrappedByteBuffer payload) {
            progressEventReceived.set(true);
            String response = payload.getString(UTF_8);
            processProgressEvent(response);
        }

        @Override
        public void requestLoaded(HttpRequest request, HttpResponse response) {
            // for Long polling. If we get an onload we have to
            // reconnect.
            if (readyState != ReadyState.CLOSED) {
                if (immediateReconnect) {
                    retry = 0;
                    if (!reconnectScheduled) {
                        reconnect();
                    }
                }
            }
        }

        @Override
        public void requestAborted(HttpRequest request) {
        }

        @Override
        public void requestClosed(HttpRequest request) {
        }
        
        @Override
        public void errorOccurred(HttpRequest request, Exception exception) {
            doError(exception);
        }
    }

    private void doOpen() {
        /**
         * Only file the event once in the case its already opened,
         * Currently, this is being called twice, once when the SSE
         * gets connected and then again when the ready state changes.
         */
        if(readyState == ReadyState.CONNECTING){
            readyState = ReadyState.OPEN;
            listener.streamOpened();
        }
    }
    
    private void doMessage(String eventName, String data) {
        // messages before OPEN and after CLOSE should not be delivered.
        if (getReadyState() != ReadyState.OPEN) {
            return;
        }
        
        listener.messageReceived(eventName, data);
    }
    
    private void doError(Exception exception) {
        if (getReadyState() == ReadyState.CLOSED) {
            return;
        }

        // TODO: Set readyState to CLOSED?
        errored = true;
        listener.streamErrored(exception);
    }
    
    public void setListener(SseEventStreamListener listener) {
        this.listener = listener;
    }
}
