#include "../include/server.h"
#include <cstdio>

Server::Server(uint32_t port) : port(port) {
	// create a server_fd AF_INET - ipv4, SOCKET_STREAM - TCP
	this -> server_fd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
	if(this -> server_fd < 0) {
		// throw error
	}

}

int8_t Server::start() {
	int32_t server_fd;
	int32_t new_socket;
	struct sockaddr_in address;
	uint32_t address_length = sizeof(address);
	int32_t options = 1;

	// const char* msg = "Hello from YSQL server\n";

	// create socket
	/*if((server_fd = socket(AF_INET, SOCK_STREAM, 0)) == 0) {
		perror("Socket failed\n");
		return -1;
	}*/

	// set socket options
	if(setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &options, sizeof(options))) {
		perror("setsockopt");
		return -1;
	}

	// bind address to port
	address.sin_family = AF_INET;
	address.sin_addr.s_addr = INADDR_ANY;
	address.sin_port = htons(port);

	if(bind(server_fd, (struct sockaddr*)&address, sizeof(address)) < 0) {
		perror("bind failed");
		return -1;
	}

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
