package com.pslog.control_plane.controller;

import com.pslog.control_plane.service.RegistryService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/v1/control")
public class BrokerApiController {

    private final RegistryService registryService;

    public BrokerApiController(RegistryService registryService) {
        this.registryService = registryService;
    }

    @PostMapping("/register-broker")
    public ResponseEntity<?> registerBroker(
            @RequestParam("id") String brokerId,
            @RequestParam("mgmt") String mgmtUrl) {

        registryService.registerBroker(brokerId, mgmtUrl);
        return ResponseEntity.ok(Map.of("status", "REGISTERED"));
    }

    @PostMapping("/topics/announce")
    public ResponseEntity<?> announceTopic(
            @RequestParam("topic") String topic,
            @RequestParam("broker_id") String brokerId) {

        registryService.announceTopic(topic, brokerId);
        return ResponseEntity.ok(Map.of("status", "ANNOUNCED"));
    }
}