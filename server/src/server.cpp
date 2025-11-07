#include "../include/server.h"
#include <cerrno>
#include <fcntl.h>

Server::Server(uint16_t port, uint8_t verbose) : port(port), verbose(verbose) {
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

std::string Server::read_message(socket_t socket) {
    if(socket < 0) {
        throw std::runtime_error(SERVER_INVALID_SOCKET_ERR_MSG);
    }
    char block[SERVER_MESSAGE_BLOCK_SIZE];

    std::string &buffer = this -> partial_buffers[socket].second;
    protocol_message_len_type& bytes_to_read = this -> partial_buffers[socket].first;

    // new instance, bytes to read must be a trash value
    if(buffer.size() < sizeof(protocol_message_len_type)) {
        bytes_to_read = 0;
    }

    while (true) {
        int32_t bytes_read = recv(socket, block, SERVER_MESSAGE_BLOCK_SIZE, 0);
        if (bytes_read < 0) {
            if(errno == EAGAIN || errno == EWOULDBLOCK) {
                break;
            }
            else {
                if(this ->verbose > 0) {
                    std::cerr << SERVER_FAILED_RECV_ERR_MSG << SERVER_ERRNO_STR_PREFIX << errno << std::endl;
                }

                partial_buffers.erase(socket);
                close(socket);
                return "";
            }

        }

        if (bytes_read == 0) {
            partial_buffers.erase(socket);
            close(socket);
            // somehow indicate that client quit
            return buffer;
        }

        buffer.append(block, bytes_read);

        if(bytes_to_read == 0 && buffer.size() >= sizeof(protocol_message_len_type)) {
            memcpy(&bytes_to_read, buffer.data(), sizeof(protocol_message_len_type));
            bytes_to_read = ntohll(bytes_to_read);
        }

        if(bytes_to_read > 0 && buffer.size() >= bytes_to_read) {
            break;
        }

        if(!(fcntl(socket, F_GETFL) & O_NONBLOCK)) {
            continue;
        }
        else {
            break;
        }
    }
    
    if (bytes_to_read > 0 && buffer.size() >= bytes_to_read) {
        std::string msg = buffer.substr(0, bytes_to_read);
        buffer.erase(0, bytes_to_read); // remove processed message
        return msg;
    }

    return ""; // message not complete yet

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
    com_code = command_ntoh(com_code);

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

socket_t Server::connect_to(const std::string& hostname, uint16_t port, bool& is_successful) const {
    // std::cout << port << std::endl;
    struct addrinfo hints{}, *res = nullptr;
    hints.ai_family = AF_INET;       // IPv4
    hints.ai_socktype = SOCK_STREAM; // TCP

    std::string port_str = std::to_string(port);
    int status = getaddrinfo(hostname.c_str(), port_str.c_str(), &hints, &res);
    if (status != 0 || !res) {
        if(this -> verbose > 0) {
            std::cerr << SERVER_FAILED_HOSTNAME_RESOLVE_ERR_MSG << hostname << ": " << gai_strerror(status) << std::endl;
        }
        is_successful = false;
        return -1;
    }

    socket_t sock = socket(res -> ai_family, res -> ai_socktype, res -> ai_protocol);
    if (sock < 0) {
        freeaddrinfo(res);
        is_successful = false;
        return -1;
    }

    if (connect(sock, res -> ai_addr, res -> ai_addrlen) != 0) {
        if(this -> verbose) {
            std::cout << SERVER_CONNECT_FAILED_ERR_MSG << SERVER_ERRNO_STR_PREFIX << errno << std::endl;
        }
        close(sock);
        freeaddrinfo(res);
        is_successful = false;
        return -1;
    }

    freeaddrinfo(res);
    is_successful = true;
    return sock;
}

std::string Server::extract_key_str_from_msg(const std::string& raw_message) const {
    // check if the command is long enough
    if(PROTOCOL_FIRST_KEY_LEN_POS + sizeof(protocol_key_len_type)  > raw_message.size()) {
        throw std::runtime_error(SERVER_MESSAGE_TOO_SHORT_ERR_MSG);
    }

    protocol_key_len_type key_len = 0;
    memcpy(&key_len, &raw_message[PROTOCOL_FIRST_KEY_LEN_POS], sizeof(key_len));
    key_len = ntohs(key_len);

    // check if msg is long enough
    if(PROTOCOL_FIRST_KEY_LEN_POS + sizeof(protocol_key_len_type) + key_len > raw_message.size()) {
        throw std::runtime_error(SERVER_MESSAGE_TOO_SHORT_ERR_MSG);
    }

    return raw_message.substr(PROTOCOL_FIRST_KEY_LEN_POS + sizeof(protocol_key_len_type), key_len);
}

void Server::make_non_blocking(socket_t& socket) {
    int flags = fcntl(socket, F_GETFL, 0);
    if(flags == -1) {
        flags = 0;
    }

    fcntl(socket, F_SETFL, flags | O_NONBLOCK);
}

int8_t Server::send_ok_response(socket_t socket) const {
    return this -> send_status_response(COMMAND_CODE_OK, socket);
}

int8_t Server::send_error_response(socket_t socket) const {
    return this -> send_status_response(COMMAND_CODE_ERR, socket);
}

int8_t Server::send_status_response(Command_Code status, socket_t socket) const {
    if(status != COMMAND_CODE_ERR && status != COMMAND_CODE_OK && status != COMMAND_CODE_DATA_NOT_FOUND) {
        return -1;
    }

    if(socket < 0) {
        return -1;
    }

    protocol_message_len_type message_length;
    protocol_array_len_type arr_len = 0;
    command_code_type com_code = command_hton(status);
    message_length = sizeof(message_length) + sizeof(arr_len) + sizeof(com_code);
    std::string message(message_length, '\0');

    protocol_message_len_type network_msg_len = protocol_msg_len_hton(message_length);

    size_t curr_pos = 0;
    memcpy(&message[0], &network_msg_len, sizeof(network_msg_len));
    curr_pos += sizeof(network_msg_len);
    memcpy(&message[curr_pos], &arr_len, sizeof(arr_len));
    curr_pos += sizeof(arr_len);
    memcpy(&message[curr_pos], &com_code, sizeof(com_code));

    try {
        this -> send_message(socket, message);
    }
    catch(const std::exception& e) {
        if(this -> verbose > 0) {
            std::cerr << e.what() << std::endl;
        }

        return -1;
    }

    return 0;
}
