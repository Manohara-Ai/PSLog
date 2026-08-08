#ifndef SCAN_HPP
#define SCAN_HPP

#include "protocol.hpp"

namespace pslog {

class Scanner {
private:
    std::string control_plane_addr;

    void print_http_body(const std::string& response) {
        size_t body_pos = response.find("\r\n\r\n");
        if (body_pos != std::string::npos) {
            std::cout << response.substr(body_pos + 4);
        } else {
            std::cout << response;
        }
    }

public:
    explicit Scanner(std::string _control_plane_addr)
        : control_plane_addr(std::move(_control_plane_addr)) {}

    void scan() {
        std::string control_plane_ip;
        int control_plane_port;

        if (!NetworkClient::parse_addr(control_plane_addr, control_plane_ip, control_plane_port)) {
            std::cerr << "[SCAN] Invalid Control Plane address: " << control_plane_addr << "\n";
            return;
        }

        int sock = NetworkClient::connect_tcp(control_plane_ip, control_plane_port);
        if (sock < 0) {
            std::cerr << "[SCAN] Failed to connect to Control Plane at " << control_plane_addr << "\n";
            return;
        }

        std::string http_req = "GET /api/v1/scan HTTP/1.1\r\n"
                               "Host: " + control_plane_ip + "\r\n"
                               "User-Agent: pslog-cli/1.0\r\n"
                               "Accept: application/json, text/plain\r\n"
                               "Connection: close\r\n\r\n";

        if (send(sock, http_req.c_str(), http_req.length(), 0) < 0) {
            perror("[SCAN] Failed to send scan request");
            close(sock);
            return;
        }

        std::cout << "[SCAN] Querying active topics from Control Plane (" << control_plane_addr << ")...\n";
        std::cout << "--------------------------------------------------\n";

        std::string full_response;
        char buffer[1024];
        ssize_t bytes_read;

        while ((bytes_read = recv(sock, buffer, sizeof(buffer) - 1, 0)) > 0) {
            buffer[bytes_read] = '\0';
            full_response.append(buffer, bytes_read);
        }

        close(sock);

        if (full_response.empty()) {
            std::cerr << "[SCAN] Received empty response from Control Plane.\n";
            return;
        }

        print_http_body(full_response);
        std::cout << "\n";
    }
};

}

#endif