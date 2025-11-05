#include "../include/server.h"
#include <cstring>

Server::Server(uint16_t port) : port(port) {
	// create a server_fd AF_INET - ipv4, SOCKET_STREAM - TCP
	this -> server_fd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
	if(this -> server_fd < 0) {
		throw std::runtime_error(SERVER_SOCKET_FAILED_ERR_MSG);
	}

	// set server options to reuse addresses if a crash happens
	int32_t options = 1;

	if(setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &options, sizeof(options))) {
		std::string setsockopt_failed_str(SERVER_SET_SOCK_OPT_FAILED_ERR_MSG);
		setsockopt_failed_str += SERVER_ERRNO_STR_PREFIX;
		setsockopt_failed_str += std::to_string(errno);
		throw std::runtime_error(setsockopt_failed_str);
	}
  
	this -> address.sin_family = AF_INET;
	this -> address.sin_addr.s_addr = INADDR_ANY;
	this -> address.sin_port = htons(port);
  
	if(bind(server_fd, (struct sockaddr*)&address, sizeof(address)) < 0) {
		std::string bind_failed_str(SERVER_BIND_FAILED_ERR_MSG);
		bind_failed_str += SERVER_ERRNO_STR_PREFIX;
		bind_failed_str += std::to_string(errno);
    	throw std::runtime_error(SERVER_BIND_FAILED_ERR_MSG);
  }
}

int8_t Server::start() {
	uint32_t new_socket;
	uint32_t address_length = sizeof(this -> address);
	const char* msg = "Hello from YSQL server\n";
	if(listen(server_fd, 3) < 0) {
		perror("listen");
		return -1;
	}

	std::cout << "Listening on port " << port << "..." << std::endl;

	while (true) {
		// Accept one client
		new_socket = accept(server_fd, (struct sockaddr*)&address, (socklen_t*)&address_length);
		if (new_socket < 0) {
			perror("accept");
			continue; // Skip this client and keep listening
		}

		// Print client info
		char client_ip[INET_ADDRSTRLEN];
		inet_ntop(AF_INET, &(address.sin_addr), client_ip, INET_ADDRSTRLEN);
		std::cout << "Client connected from " << client_ip << ":" << ntohs(address.sin_port) << std::endl;

		// Send message
		send(new_socket, msg, strlen(msg), 0);
		std::cout << "Hello message sent!" << std::endl;

		// Close client socket
		close(new_socket);
	}
	
	return 0;
}

std::string Server::read_message(socket_t socket) const {
    if(socket < 0) {
        throw std::runtime_error(SERVER_INVALID_SOCKET_ERR_MSG);
    }

    std::string buffer;
    char block[SERVER_MESSAGE_BLOCK_SIZE];

    uint64_t bytes_to_read = 0;

    while (buffer.size() < sizeof(uint64_t)) {
        int32_t bytes_read = recv(socket, block, SERVER_MESSAGE_BLOCK_SIZE, 0);
        if (bytes_read < 0) {
            std::string recv_failed_str(SERVER_FAILED_RECV_ERR_MSG);
            recv_failed_str += SERVER_ERRNO_STR_PREFIX;
            recv_failed_str += std::to_string(errno);
            throw std::runtime_error(recv_failed_str);
        }

        if (bytes_read == 0) {
            return buffer;
        }

        buffer.append(block, bytes_read);
    }

    memcpy(&bytes_to_read, buffer.data(), sizeof(uint64_t));
    
    // not sure for this part but for now the first 8 bytes do not contain themselsevse
    bytes_to_read += sizeof(uint64_t);

    while (buffer.size() < bytes_to_read) {
        int32_t bytes_read = recv(socket, block, SERVER_MESSAGE_BLOCK_SIZE, 0);
        if (bytes_read < 0) {
            std::string recv_failed_str(SERVER_FAILED_RECV_ERR_MSG);
            recv_failed_str += SERVER_ERRNO_STR_PREFIX;
            recv_failed_str += std::to_string(errno);
            throw std::runtime_error(recv_failed_str);
        }

        if (bytes_read == 0) {
            return buffer;
        }

        buffer.append(block, bytes_read);
    }

    return buffer.substr(0, bytes_to_read);
}

int64_t Server::send_message(socket_t socket, const std::string& message) const {
    if(socket < 0) {
        throw std::runtime_error(SERVER_INVALID_SOCKET_ERR_MSG);
    }

    size_t total_sent = 0;
    ssize_t sent_bytes;

    while (total_sent < message.size()) {
        sent_bytes = send(socket, message.data() + total_sent, message.size() - total_sent, 0);

        if (sent_bytes < 0) {
            if (errno == EINTR) {
                continue; 
            }
            std::string send_failed_str(SERVER_FAILED_SEND_ERR_MSG);
            send_failed_str += SERVER_ERRNO_STR_PREFIX;
            send_failed_str += std::to_string(errno);
            throw std::runtime_error(send_failed_str);
        }

        if (sent_bytes == 0) {
            break;
        } 

        total_sent += sent_bytes;
    }

    return static_cast<int64_t>(total_sent);
}

Command_Code Server::extract_command_code(const std::string& raw_message) const {
    if(PROTOCOL_COMMAND_NUMBER_POS + sizeof(command_code_type) > raw_message.size()) {
        return INVALID_COMMAND_CODE;
    }

    command_code_type com_code;
    memcpy(&com_code, &raw_message[PROTOCOL_COMMAND_NUMBER_POS], sizeof(command_code_type));

    return static_cast<Command_Code>(com_code);
}

bool Server::try_connect(const std::string& hostname, uint16_t port, uint32_t timeout_sec) const {
    bool success = false;

    socket_t sock = this -> connect_to(hostname, port, success);

    if(success) {
        close(sock);
    }

    return success;
}

#include <netdb.h>      // getaddrinfo, freeaddrinfo
#include <unistd.h>     // close
#include <cstring>      // memset, memcpy
#include <arpa/inet.h>  // htons

socket_t Server::connect_to(const std::string& hostname, uint16_t port, bool& is_successful) const {
    struct addrinfo hints{}, *res = nullptr;
    hints.ai_family = AF_INET;       // IPv4
    hints.ai_socktype = SOCK_STREAM; // TCP

    std::string port_str = std::to_string(port);
    int status = getaddrinfo(hostname.c_str(), port_str.c_str(), &hints, &res);
    if (status != 0 || !res) {
        #ifdef SERVER_DEBUG
        std::cerr << SERVER_FAILED_HOSTNAME_RESOLVE << hostname << ": " << gai_strerror(status) << std::endl;
        #endif
        is_successful = false;
        return -1;
    }

    socket_t sock = socket(res -> ai_family, res -> ai_socktype, res -> ai_protocol);
    if (sock < 0) {
        freeaddrinfo(res);
        is_successful = false;
        return -1;
    }

    if (connect(sock, res->ai_addr, res->ai_addrlen) != 0) {
        close(sock);
        freeaddrinfo(res);
        is_successful = false;
        return -1;
    }

    freeaddrinfo(res);
    is_successful = true;
    return sock;
}

