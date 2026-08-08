package com.pslog.control_plane.controller;

import com.pslog.control_plane.service.RegistryService;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;
import java.util.Set;

@RestController
@RequestMapping("/api/v1")
public class ClientApiController {

    private final RegistryService registryService;

    public ClientApiController(RegistryService registryService) {
        this.registryService = registryService;
    }

    @PostMapping("/subscribe")
    public ResponseEntity<?> subscribe(
            @RequestParam("topic") String topic,
            @RequestParam("port") int udpPort,
            HttpServletRequest request) {

        String clientIp = request.getRemoteAddr();
        if ("0:0:0:0:0:0:0:1".equals(clientIp)) {
            clientIp = "127.0.0.1";
        }

        boolean registered = registryService.registerSubscriberWithBroker(topic, clientIp, udpPort);

        if (registered) {
            return ResponseEntity.ok(Map.of("status", "SUCCESS", "topic", topic, "client_ip", clientIp, "udp_port", udpPort));
        } else {
            return ResponseEntity.status(503).body(Map.of("status", "ERROR", "message", "No brokers available"));
        }
    }

    @GetMapping("/scan")
    public ResponseEntity<?> scan() {
        Set<String> topics = registryService.getActiveTopics();
        
        StringBuilder output = new StringBuilder();
        output.append("Active PSLog Topics:\n");
        if (topics.isEmpty()) {
            output.append("  (No active topics found)\n");
        } else {
            for (String topic : topics) {
                output.append("  • ").append(topic).append("\n");
            }
        }

        return ResponseEntity.ok(output.toString());
    }
}