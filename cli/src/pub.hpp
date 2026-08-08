#ifndef PUB_HPP
#define PUB_HPP

#include "protocol.hpp"

namespace pslog {

class Publisher {
private:
    std::string topic;
    std::string socket_path;
    int sock = -1;

    bool send_framed_msg(const std::string& json_payload) {
        uint32_t len = htonl(static_cast<u_int32_t>(json_payload.length()));

        if (send(sock, &len, sizeof(len), 0) < 0)
            return false;

        if (send(sock, json_payload.c_str(), json_payload.length(), 0) < 0)
            return false;

        return true;
    }

    uint64_t get_timestamp() {
        using namespace std::chrono;
        return duration_cast<seconds>(system_clock::now().time_since_epoch()).count();
    }

public:
    Publisher(std::string _topic, std::string _socket_path) 
        : topic(std::move(_topic)), socket_path(std::move(_socket_path)) {}

    ~Publisher() {
        if (sock >= 0) {
            close(sock);
        }
    }

    void run(const std::string& exec_cmd) {
        sock = NetworkClient::ensure_broker(socket_path);
        if (sock < 0)
            return;

        std::string pub_msg = "{\"type\":\"Pub\",\"topic\":\"" + topic + "\"}";
        if (!send_framed_msg(pub_msg)) {
            std::cerr << "[PUB] Failed to send handshake frame to broker.\n";
            return;
        }

        std::cout << "[PUB] Streaming output from '" << exec_cmd << "' to topic '" << topic << "'\n";
        FILE* fp = popen(exec_cmd.c_str(), "r");
        if (!fp) {
            perror("Failed to execute child process");
            return;
        }

        char buffer[1024];
        while (fgets(buffer, sizeof(buffer), fp) != nullptr) {

            std::string line(buffer);
            if (!line.empty() && line.back() == '\n') line.pop_back();

            uint64_t ts = get_timestamp();
            std::string log_msg = "{\"type\":\"Log\",\"log\":{\"ts\":" + std::to_string(ts) + 
                                  ",\"level\":\"INFO\",\"message\":\"" + line + "\"}}";

            if (!send_framed_msg(log_msg)) {
                std::cerr << "[PUB] Broker connection broken.\n";
                break;
            }
        }

        pclose(fp);
    }
};

} //namespace pslog

#endif