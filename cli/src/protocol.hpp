#ifndef PROTOCOL_HPP
#define PROTOCOL_HPP

#include <iostream>
#include <string>
#include <cstring>
#include <fstream>
#include <sstream>
#include <vector>
#include <chrono>
#include <thread>
#include <cstdio>
#include <cstdlib>
#include <cerrno>

#include <unistd.h>
#include <sys/un.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netinet/in.h>

namespace pslog {

struct Config {
    std::string control_plane_addr = "127.0.0.1:8080";
    std::string broker_addr        = "/tmp/pslog.sock";
};

class ConfigManager {
public:
    static Config load() {
        Config config;
        std::vector<std::string> paths = {
            "pslog.conf",
            std::string(getenv("HOME") ? getenv("HOME") : "") + "/.pslog.conf"
        };

        for (const auto& path : paths) {
            std::ifstream file(path);
            if (!file.is_open()) 
                continue;

            std::string line;
            while (std::getline(file, line)) {
                if (line.empty() || line[0] == '#') 
                    continue;

                std::stringstream is_line(line);
                std::string key, value;

                if (std::getline(is_line, key, '=') && 
                    std::getline(is_line, value)) {

                    if (key == "CONTROL_PLANE_ADDR") 
                        config.control_plane_addr = value;

                    else if (key == "BROKER_ADDR") 
                        config.broker_addr = value; 
                }
            }
            break;
        }

        return config;
    }
};

class NetworkClient {
public:
    static bool parse_addr(const std::string& addr_str, std::string& ip, int& port) {
        size_t pos = addr_str.find(':');
        if (pos == std::string::npos)
            return false;

        ip = addr_str.substr(0, pos);
        try {
            port = std::stoi(addr_str.substr(pos + 1));
        } catch (...) {
            return false;
        }
        
        return true;
    }

    static int connect_tcp(const std::string& host, int port) {
        int sock = socket(AF_INET, SOCK_STREAM, 0);
        if (sock < 0) return -1;

        sockaddr_in server_addr{};
        server_addr.sin_family = AF_INET;
        server_addr.sin_port = htons(port);

        if (inet_pton(AF_INET, host.c_str(), &server_addr.sin_addr) <= 0) {
            close(sock);
            return -1;
        }

        if (connect(sock, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
            close(sock);
            return -1;
        }

        return sock;
    }

    static int connect_unix(const std::string& socket_path, bool quiet = false) {
        int sock = socket(AF_UNIX, SOCK_STREAM, 0);
        
        if (sock < 0) {
            if (!quiet) perror("Unix socket creation failed");
            return -1;
        }

        sockaddr_un addr{};
        addr.sun_family = AF_UNIX;
        std::strncpy(addr.sun_path, socket_path.c_str(), sizeof(addr.sun_path) - 1);

        if (connect(sock, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
            if (!quiet) perror("Unix socket connection failed");
            close(sock);
            return -1;
        }

        return sock;
    }

    static int ensure_broker(const std::string& socket_path) {
        int sock = connect_unix(socket_path, true);
        if (sock >= 0) 
            return sock;

        std::cout << "[PSLog] Broker is not online at " << socket_path << ". Spawning process...\n";
        std::system("nohup ./bin/broker > /dev/null 2>&1 &");

        for (int i = 0; i < 20; i++) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            sock = connect_unix(socket_path, true);
            if (sock >= 0)
                return sock;
        }

        std::cerr << "[ERROR] Failed to connect to or spawn broker.\n";
        return -1;
    }
};

} // namespace pslog

#endif