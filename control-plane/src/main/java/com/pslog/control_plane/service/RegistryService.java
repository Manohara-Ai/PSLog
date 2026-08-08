package com.pslog.control_plane.service;

import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class RegistryService {
    
    private final RestTemplate restTemplate = new RestTemplate();
    private final Map<String, String> brokers = new ConcurrentHashMap<>();
    private final Map<String, Set<String>> topicBrokers = new ConcurrentHashMap<>();

    public void registerBroker(String id, String mgmtUrl) {
        brokers.put(id, mgmtUrl);
        System.out.println("[CONTROL PLANE] Registered broker: " + id + " at " + mgmtUrl);
    }

    public void announceTopic(String topic, String brokerId) {
        topicBrokers.computeIfAbsent(topic, k -> ConcurrentHashMap.newKeySet()).add(brokerId);
        System.out.println("[CONTROL PLANE] Topic '" + topic + "' announced by broker " + brokerId);
    }

    public boolean registerSubscriberWithBroker(String topic, String clientIp, int udpPort) {
        Set<String> hostingBrokers = topicBrokers.get(topic);
        
        if (hostingBrokers == null || hostingBrokers.isEmpty()) {
            if (brokers.containsKey("broker-1")) {
                hostingBrokers = Set.of("broker-1");
            } else {
                System.err.println("[CONTROL PLANE WARN] No brokers available to route subscriber for topic: " + topic);
                return false;
            }
        }

        boolean success = false;
        for (String brokerId : hostingBrokers) {
            String mgmtUrl = brokers.get(brokerId);
            if (mgmtUrl != null) {
                try {
                    String url = String.format("%s/api/v1/broker/register?topic=%s&ip=%s&port=%d",
                            mgmtUrl, topic, clientIp, udpPort);
                    restTemplate.postForEntity(url, null, String.class);
                    System.out.println("[CONTROL PLANE] Routed subscriber for '" + topic + "' to " + brokerId);
                    success = true;
                } catch (Exception e) {
                    System.err.println("[CONTROL PLANE ERROR] Failed to register subscriber with broker " + brokerId + ": " + e.getMessage());
                }
            }
        }
        return success;
    }

    public Set<String> getActiveTopics() {
        return topicBrokers.keySet();
    }
}
