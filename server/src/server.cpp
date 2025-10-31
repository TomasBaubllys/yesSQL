#include "../include/server.h"

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
