package br.com.dojot.IoTAgent;

import br.com.dojot.utils.Services;
import jdk.nashorn.internal.parser.JSONParser;
import org.json.JSONException;
import org.json.JSONObject;
import org.apache.log4j.Logger;

import java.time.Instant;
import java.util.function.BiFunction;

import com.mycompany.app.messenger.Messenger;
import com.mycompany.app.config.Config;


public class IoTAgent {
    Logger mLogger = Logger.getLogger(IoTAgent.class);
    Messenger messenger = new Messenger();

    public IoTAgent() {
        mLogger.info("Initializing Messenger for Iotagent-java...");
        this.messenger.init();
        mLogger.info("... Messenger was successfully initialized.");
        mLogger.info("creating channel...");
        this.messenger.createChannel(Config.getInstance().getIotagentDefaultSubject(),"w",false);
        this.messenger.createChannel(Config.getInstance().getDeviceManagerDefaultSubject(), "rw", false);
        this.messenger.on(Config.getInstance().getDeviceManagerDefaultSubject(),"message", (ten,msg) -> { this.callback(ten,msg); return null;});
        this.messenger.generateDeviceCreateEventForActiveDevices();

    }

    public void callback(String tenant, String message) {
        JSONObject messageObj = new JSONObject(message);

        String eventType = "device." + messageObj.get("event").toString();
        System.out.println(messageObj.toString());
        this.messenger.emit("iotagent.device", tenant, eventType, messageObj.toString());

    }

    public void updateAttrs(String deviceId, String tenant, JSONObject attrs, JSONObject metadata) {
        if (metadata == null) {
            metadata = new JSONObject();
        }
        this.checkCompleteMetaFields(deviceId, tenant, metadata);
        JSONObject event = new JSONObject();
        event.put("metadata", metadata);
        event.put("attrs", attrs);
        this.messenger.publish(Config.getInstance().getIotagentDefaultSubject(), tenant, event.toString());
    }

    private void checkCompleteMetaFields(String deviceId, String tenant, JSONObject metadata) {
        if (!metadata.has("deviceid")) {
            metadata.put("deviceid", deviceId);
        }

        if (!metadata.has("tenant")) {
            metadata.put("tenant", tenant);
        }

        if (!metadata.has("timestamp")) {
            Long now = Instant.now().toEpochMilli();
            metadata.put("timestamp", now);
        }

        if (!metadata.has("templates")) {
            JSONObject device = Services.getInstance().getDevice(deviceId, tenant);
            if (device != null) {
                try {
                    metadata.put("templates", device.get("templates"));
                } catch (JSONException exception) {
                    mLogger.error("Json error: " + exception);
                }
            } else {
                mLogger.error("Cannot get templates for deviceId: " + deviceId);
            }
        }
    }


    public void on (String subject, String event, BiFunction<String, String, Void> callback){
        this.messenger.createChannel(subject, "r", false );
        this.messenger.on(subject, event, callback);
    }

}
