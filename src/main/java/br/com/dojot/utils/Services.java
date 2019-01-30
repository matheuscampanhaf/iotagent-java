package br.com.dojot.utils;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import com.cpqd.app.config.Config;
import com.cpqd.app.auth.Auth;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class Services {
    Logger mLogger = Logger.getLogger(Services.class);
    private static Services mInstance;
    private Cache<String, JSONObject> mCache;

    private Services() {
        this.mCache = Caffeine.newBuilder()
                .expireAfterAccess(1, TimeUnit.MINUTES)
                .build();
    }

    public static synchronized Services getInstance() {
        if (mInstance == null) {
            mInstance = new Services();
        }
        return mInstance;
    }

    public synchronized void addDeviceToCache(String key, JSONObject data) {
        this.mCache.put(key, data);
    }

    public synchronized void removeDeviceFromCache(String key) {
        this.mCache.invalidate(key);
    }


    //TODO: pagination?
    public List<String> listDevices(String tenant) {
        List<String> devices = new ArrayList<>();

        StringBuilder url = new StringBuilder(Config.getInstance().getDeviceManagerAddress());
        url.append("/device?idsOnly");
        try {
            HttpResponse<JsonNode> request = Unirest.get(url.toString())
                    .header("authorization", "Bearer " + Auth.getInstance().getToken(tenant))
                    .asJson();

            JSONArray jsonResponse = request.getBody().getArray();
            for (int i = 0; i < jsonResponse.length(); i++) {
                devices.add(jsonResponse.getString(i));
            }
            return devices;
        } catch (UnirestException exception) {
            mLogger.error("Cannot get url[" + url.toString() + "]");
            mLogger.error("Failed to acquire existing devices");
            mLogger.error("Error: " + exception.toString());
        } catch (JSONException exception) {
            mLogger.error("Json error: " + exception.toString());
        }

        return null;
    }

    public JSONObject getDevice(String deviceId, String tenant) {
        String key = this.getCacheKey(tenant, deviceId);

        JSONObject cached = this.mCache.getIfPresent(key);
        if (cached != null) {
            mLogger.debug("Device [" + deviceId + "] is already cached for tenant " + tenant);
            return cached;
        }

        StringBuilder url = new StringBuilder(Config.getInstance().getDeviceManagerAddress());
        url.append("/internal/device/");
        url.append(deviceId);
        try {
            HttpResponse<JsonNode> response = Unirest.get(url.toString())
                    .header("authorization", "Bearer " + Auth.getInstance().getToken(tenant))
                    .asJson();
            if ((response.getStatus() >= 200) && (response.getStatus() < 300)) {
            	JSONObject deviceResponse = response.getBody().getObject();
            	this.mCache.put(key, deviceResponse);
            	return deviceResponse;
            } else {
            	mLogger.error("Cannot get device[" + deviceId + "] Response: " + String.valueOf(response.getStatus()));
            	return null;
            }
        } catch (UnirestException exception) {
            mLogger.error("Cannot get url[" + url.toString() + "]");
            mLogger.error("Failed to acquire existing tenancy contexts");
            mLogger.error("Error: " + exception.toString());
        } catch (JSONException exception) {
            mLogger.error("Json error: " + exception.toString());
        }

        return null;
    }

    public String getCacheKey(String tenant, String deviceId) {
        StringBuilder response = new StringBuilder("device:");
        response.append(tenant);
        response.append(":");
        response.append(deviceId);
        return response.toString();
    }

    public static void main (String[] args){
        Services.getInstance().listDevices("admin");
    }
}
