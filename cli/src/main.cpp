#include <iostream>
#include <string>

#include "protocol.hpp"
#include "pub.hpp"
#include "sub.hpp"
#include "scan.hpp"

void print_usage(const char* prog_name) {
    std::cout << "PSLog Distributed Logging CLI\n\n"
              << "Usage:\n"
              << "  " << prog_name << " pub  <topic> [command] [broker_socket_path]\n"
              << "  " << prog_name << " sub  <topic> [control_plane_ip:port]\n"
              << "  " << prog_name << " scan [control_plane_ip:port]\n\n"
              << "Examples:\n"
              << "  " << prog_name << " pub auth-service \"ping 8.8.8.8\"\n"
              << "  " << prog_name << " sub auth-service\n"
              << "  " << prog_name << " scan\n";
}

int main(int argc, char* argv[]) {
    if (argc < 2) {
        print_usage(argv[0]);
        return 1;
    }

        pslog::Config config = pslog::ConfigManager::load();
    std::string mode = argv[1];

    if (mode == "pub") {
        if (argc < 3) {
            std::cerr << "[ERROR] pub requires at least <topic>\n\n";
            print_usage(argv[0]);
            return 1;
        }

        std::string topic = argv[2];
        std::string command = (argc >= 4) ? argv[3] : "ping -c 5 8.8.8.8";
        std::string broker_socket = (argc >= 5) ? argv[4] : config.broker_addr;

        pslog::Publisher publisher(topic, broker_socket);
        publisher.run(command);

    } else if (mode == "sub") {
        if (argc < 3) {
            std::cerr << "[ERROR] sub requires at least <topic>\n\n";
            print_usage(argv[0]);
            return 1;
        }

        std::string topic = argv[2];
        std::string cp_addr = (argc >= 4) ? argv[3] : config.control_plane_addr;

        pslog::Subscriber subscriber(topic, cp_addr);
        subscriber.listen();

    } else if (mode == "scan") {
        std::string cp_addr = (argc >= 3) ? argv[2] : config.control_plane_addr;

        pslog::Scanner scanner(cp_addr);
        scanner.scan();

    } else if (mode == "-h" || mode == "--help") {
        print_usage(argv[0]);
        return 0;

    } else {
        std::cerr << "[ERROR] Unknown command: " << mode << "\n\n";
        print_usage(argv[0]);
        return 1;
    }

    return 0;
}