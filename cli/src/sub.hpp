#ifndef SUB_HPP
#define SUB_HPP

#include "protocol.hpp"

namespace pslog {

class Subscriber {
private:
    std::string topic;
    std::string control_plane_addr;

    bool register_with_control_plane(int local_udp_port) {
        std::string control_plane_ip;
        int control_plane_port;

        if (!NetworkClient::parse_addr(control_plane_addr, control_plane_ip, control_plane_port)) {
            std::cerr << "[SUB] Invalid Control Plane address: " << control_plane_addr << "\n";
            return false;
        }

        int http_sock = NetworkClient::connect_tcp(control_plane_ip, control_plane_port);
        if (http_sock < 0) {
            std::cerr << "[SUB] Failed to connect to Control Plane at " << control_plane_addr << "\n";
            return false;
        }

        std::string path = "/api/v1/subscribe?topic=" + topic + "&port=" + std::to_string(local_udp_port);
        std::string http_req = "POST " + path + " HTTP/1.1\r\n" +
                               "Host: " + control_plane_ip + "\r\n" +
                               "Content-Length: 0\r\n" +
                               "Connection: close\r\n\r\n";

        send(http_sock, http_req.c_str(), http_req.length(), 0);
        close(http_sock);
        return true;
    }

public:
    Subscriber(std::string _topic, std::string _control_plane_addr)
        : topic(std::move(_topic)), control_plane_addr(std::move(_control_plane_addr)) {}

    void listen() {
        int udp_sock = socket(AF_INET, SOCK_DGRAM, 0);
        if (udp_sock < 0) {
            perror("[SUB] Failed to create UDP socket");
            return;
        }

        sockaddr_in local_addr{};
        local_addr.sin_family = AF_INET;
        local_addr.sin_addr.s_addr = INADDR_ANY;
        local_addr.sin_port = htons(0);

        if (bind(udp_sock, (struct sockaddr*)&local_addr, sizeof(local_addr)) < 0) {
            perror("[SUB] UDP Bind failed");
            close(udp_sock);
            return;
        }

        socklen_t addr_len = sizeof(local_addr);
        getsockname(udp_sock, (struct sockaddr*)&local_addr, &addr_len);
        int local_port = ntohs(local_addr.sin_port);

        std::cout << "[SUB] Bound local UDP socket to port " << local_port << "\n";

        if (!register_with_control_plane(local_port)) {
            close(udp_sock);
            return;
        }

        std::cout << "[SUB] Registered topic '" << topic << "' via Control Plane.\n";
        std::cout << "--------------------------------------------------\n";

        char buffer[2048];
        sockaddr_in src_addr{};
        socklen_t src_len = sizeof(src_addr);

        while (true) {
            ssize_t bytes_read = recvfrom(udp_sock, buffer, sizeof(buffer) - 1, 0,
                                         (struct sockaddr*)&src_addr, &src_len);
            if (bytes_read < 0) {
                perror("[SUB] UDP recv error");
                break;
            }

            buffer[bytes_read] = '\0';
            std::cout << buffer << std::flush;
        }

        close(udp_sock);
    }
};

} //namespace pslog

#endif