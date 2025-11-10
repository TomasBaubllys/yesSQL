#include "../include/server.h"
#include <cerrno>
#include <fcntl.h>
#include <sys/eventfd.h>

Server::Server(uint16_t port, uint8_t verbose) : port(port), verbose(verbose), epoll_events(SERVER_DEFAULT_EPOLL_EVENT_VAL), thread_pool(SERVER_DEFAULT_THREAD_POOL_VAL) {
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

Server::~Server() {
    /*
    CLOSE ALL SOCKETS
    
    */
    if (this -> epoll_fd >= 0) {
        close(this -> epoll_fd);
    }

    if(this -> server_fd > 0) {
        close(this -> server_fd);
    }
    this -> sockets_type_map.clear();
}

int8_t Server::start() {

    return 0;
}

Server_Message Server::read_message(socket_t socket_fd) {
    if(socket_fd < 0) {
        throw std::runtime_error(SERVER_INVALID_SOCKET_ERR_MSG);
    }
    char block[SERVER_MESSAGE_BLOCK_SIZE];

    // Server_Request serv_req = this -> partial_read_buffers[socket_fd];
    std::unordered_map<socket_t, Server_Request>::iterator serv_req_iter = this -> partial_read_buffers.find(socket_fd);
    if(serv_req_iter == this -> partial_read_buffers.end()) {
         std::pair<std::unordered_map<socket_t, Server_Request>::iterator, bool> new_iter = this -> partial_read_buffers.emplace(socket_fd, Server_Request{});
        serv_req_iter = new_iter.first;
    }

    bool header_has_client_id = false;
    std::unordered_map<socket_t, Fd_Type>::iterator sit = this -> sockets_type_map.find(socket_fd);
    if(sit != this -> sockets_type_map.end() && sit -> second != Fd_Type::CLIENT) {
        header_has_client_id = true;
    }

    if(serv_req_iter -> second.message.size() < sizeof(protocol_message_len_type) + sizeof(protocol_id_t)) {
        serv_req_iter -> second.bytes_to_process = 0;
    }

    while (true) {
        int32_t bytes_read = recv(socket_fd, block, SERVER_MESSAGE_BLOCK_SIZE, 0);
        //std::cout << "bytes received" << bytes_read <<  std::endl;

        if (bytes_read < 0) {
            if(errno == EAGAIN || errno == EWOULDBLOCK) {
                break;
            }
            else {
                if(this -> verbose > 0) {
                    std::cerr << SERVER_FAILED_RECV_ERR_MSG << SERVER_ERRNO_STR_PREFIX << errno << std::endl;
                }

                /** if by context its a partition type clean eveyrything up
                 * 
                 * 
                 */

                partial_read_buffers.erase(socket_fd);
                epoll_ctl(this -> epoll_fd, EPOLL_CTL_DEL, socket_fd, nullptr);
                close(socket_fd);
                Server_Message serv_msg;
                serv_msg.message = "";
                return serv_msg;
            }
        }
        if (bytes_read == 0) {
                /** if by context its a partition type clean eveyrything up
                 * 
                 * 
                 */

            partial_read_buffers.erase(socket_fd);
            epoll_ctl(this -> epoll_fd, EPOLL_CTL_DEL, socket_fd, nullptr);
            close(socket_fd);
            // somehow indicate that client quit
            Server_Message serv_msg;
            serv_msg.message = "";
            return serv_msg; // buffer;
        }

        serv_req_iter -> second.message.append(block, bytes_read);

        if(serv_req_iter -> second.bytes_to_process == 0 && serv_req_iter -> second.message.size() >= sizeof(protocol_message_len_type) + sizeof(protocol_id_t)) {
            const char* data = serv_req_iter -> second.message.data();
            memcpy(&serv_req_iter -> second.bytes_to_process, data, sizeof(protocol_message_len_type));
            serv_req_iter -> second.bytes_to_process = ntohll(serv_req_iter -> second.bytes_to_process);
            if(header_has_client_id) {
                memcpy(&serv_req_iter -> second.client_id, serv_req_iter -> second.message.data() + sizeof(protocol_message_len_type), sizeof(protocol_id_t));
                serv_req_iter -> second.client_id = protocol_id_ntoh(serv_req_iter -> second.client_id);
                // serv_req_iter -> second.message.erase(sizeof(protocol_message_len_type), sizeof(protocol_id_t));
            }
        }


        if(serv_req_iter -> second.bytes_to_process > 0 && serv_req_iter -> second.message.size() >= serv_req_iter -> second.bytes_to_process) {
            break;
        }
    }

    if (serv_req_iter -> second.bytes_to_process > 0 && serv_req_iter -> second.message.size() >= serv_req_iter -> second.bytes_to_process) {
        Server_Message msg = serv_req_iter -> second;
        serv_req_iter -> second.bytes_processed = 0;
        serv_req_iter -> second.bytes_to_process = 0;
        serv_req_iter -> second.message.clear();
        serv_req_iter -> second.client_id = 0;
        /*for(int i = 0; i < 34; ++i) {
            std::cout << int(msg.message[i]) << " ";
        }
                std::cout << std::endl;
        */
        return msg;
    }

    Server_Message serv_msg;
    serv_msg.message = "";
    return serv_msg; // message not complete yet

}

int64_t Server::send_message(socket_t socket_fd, Server_Response& server_response) {
    if (socket_fd < 0) {
        throw std::runtime_error(SERVER_INVALID_SOCKET_ERR_MSG);
    }

    size_t offset = server_response.bytes_processed;
    size_t remaining = 0;

    if (server_response.bytes_to_process > server_response.bytes_processed) {
        remaining = server_response.bytes_to_process - server_response.bytes_processed;
    }
    else {
        return 0; // nothing left to send
    }

    const char* data_ptr = server_response.message.data() + offset;
    ssize_t bytes_sent = send(socket_fd, data_ptr, remaining, MSG_NOSIGNAL);
    //std::cout << "bytes sent" << std::endl;

    if (bytes_sent < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            return 0; // try again later
        } else {
            if (verbose > 0)
                std::cerr << SERVER_FAILED_SEND_ERR_MSG << SERVER_ERRNO_STR_PREFIX << errno << std::endl;
            return -1; // fatal error, caller should close socket
        }
    }

    server_response.bytes_processed += bytes_sent;
    return bytes_sent;
}


Command_Code Server::extract_command_code(const std::string& message, bool contains_cid) const {
    uint64_t pos = contains_cid? PROTOCOL_COMMAND_NUMBER_POS : PROTOCOL_COMMAND_NUMBER_POS_NOCID;

    if(pos + sizeof(command_code_type) > message.size()) {
        return INVALID_COMMAND_CODE;
    }

    command_code_type com_code;
    memcpy(&com_code, &message[pos], sizeof(command_code_type));
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

std::string Server::extract_key_str_from_msg(const std::string& message, bool contains_cid) const {
    // check if the command is long enough
    uint64_t pos = contains_cid? PROTOCOL_FIRST_KEY_LEN_POS : PROTOCOL_FIRST_KEY_LEN_POS_NOCID;

    if(pos + sizeof(protocol_key_len_type)  > message.size()) {
        throw std::runtime_error(SERVER_MESSAGE_TOO_SHORT_ERR_MSG);
    }

    protocol_key_len_type key_len = 0;
    memcpy(&key_len, &message[pos], sizeof(key_len));
    key_len = ntohs(key_len);

    // check if msg is long enough
    if(pos + sizeof(protocol_key_len_type) + key_len > message.size()) {
        throw std::runtime_error(SERVER_MESSAGE_TOO_SHORT_ERR_MSG);
    }

    return message.substr(pos + sizeof(protocol_key_len_type), key_len);
}

void Server::make_non_blocking(socket_t& socket) {
    int flags = fcntl(socket, F_GETFL, 0);
    if(flags == -1) {
        flags = 0;
    }

    fcntl(socket, F_SETFL, flags | O_NONBLOCK);
}

Server_Message Server::create_ok_response(bool contain_cid, protocol_id_t client_id) const {
    return this -> create_status_response(COMMAND_CODE_OK, contain_cid, client_id);
}

Server_Message Server::create_error_response(bool contain_cid, protocol_id_t client_id) const {
    return this -> create_status_response(COMMAND_CODE_ERR, contain_cid, client_id);
}

Server_Message Server::create_status_response(Command_Code status, bool contain_cid, protocol_id_t client_id) const {
    if(status != COMMAND_CODE_ERR && status != COMMAND_CODE_OK && status != COMMAND_CODE_DATA_NOT_FOUND) {
        Server_Message empty_msg;
        empty_msg.message = "";
        return empty_msg;
    }

    protocol_message_len_type message_length;
    protocol_array_len_type arr_len = 0;
    command_code_type com_code = command_hton(status);
    message_length = sizeof(message_length) + sizeof(arr_len) + sizeof(com_code);
    if(contain_cid) {
        message_length += sizeof(protocol_id_t);
    }
    std::string message(message_length, '\0');

    protocol_message_len_type network_msg_len = protocol_msg_len_hton(message_length);

    size_t curr_pos = 0;
    memcpy(&message[0], &network_msg_len, sizeof(network_msg_len));
    curr_pos += sizeof(network_msg_len);
    if(contain_cid) {
        protocol_id_t net_cid = protocol_id_hton(client_id);
        memcpy(&message[curr_pos], &net_cid, sizeof(net_cid));
        curr_pos += sizeof(net_cid);
    }
    memcpy(&message[curr_pos], &arr_len, sizeof(arr_len));
    curr_pos += sizeof(arr_len);
    memcpy(&message[curr_pos], &com_code, sizeof(com_code));

    Server_Message resp;
    resp.client_id = client_id;
    resp.bytes_processed = 0;
    resp.bytes_to_process = message.size();
    resp.message = message;

    return resp;
}

void Server::init_epoll() {
    this -> epoll_fd = epoll_create1(0);
    
    this -> wakeup_fd = eventfd(0, EFD_NONBLOCK);
    if(this -> wakeup_fd < 0) {
        std::cout << "ERROR WAKE UP IS NOT VALID" << std::endl;
    }

    epoll_event ev;
    ev.events = EPOLLIN;
    ev.data.fd = this -> wakeup_fd;
    epoll_ctl(this->epoll_fd, EPOLL_CTL_ADD, this -> wakeup_fd, &ev);

    if(this -> epoll_fd < 0) {
        throw std::runtime_error(SERVER_FAILED_EPOLL_CREATE_ERR_MSG);
    }
}

std::vector<socket_t> Server::add_client_socket_to_epoll() {
    std::vector<socket_t> client_fds;

    while(true) {
        sockaddr_in client_addr{};
        socklen_t len = sizeof(client_addr);

        socket_t client_fd = accept(server_fd, (sockaddr*)&client_addr, &len);
        if(client_fd < 0) {
            if(errno == EAGAIN || errno == EWOULDBLOCK) {
                break;
            }

            return {};
        }

        epoll_event ep_ev{};
        this -> make_non_blocking(client_fd);
        
        ep_ev.events = EPOLLIN;// | EPOLLET;
        ep_ev.data.fd = client_fd;
        if(epoll_ctl(this -> epoll_fd, EPOLL_CTL_ADD, client_fd, &ep_ev) < 0) {
            throw std::runtime_error(SERVER_FAILED_EPOLL_ADD_FAILED_ERR_MSG);
        };

        client_fds.push_back(client_fd);
    }

    return client_fds;
}

int32_t Server::server_epoll_wait() {
    return epoll_wait(this -> epoll_fd, this -> epoll_events.data(), this -> epoll_events.size(), -1);
}

void Server::add_this_to_epoll() {
    this -> make_non_blocking(this -> server_fd);
    this -> sockets_type_map[this -> server_fd] = Fd_Type::LISTENER;
    epoll_event ep_ev{};
    ep_ev.events = EPOLLIN;
    ep_ev.data.fd = this -> server_fd;
    if(epoll_ctl(this -> epoll_fd, EPOLL_CTL_ADD, this -> server_fd, &ep_ev) < 0) {
        throw std::runtime_error(SERVER_FAILED_EPOLL_ADD_FAILED_ERR_MSG);
    }
}

void Server::request_to_remove_fd(socket_t socket) {
    std::lock_guard<std::mutex> lock(this -> remove_mutex);
    this -> remove_queue.push_back(socket);
}
        
void Server::process_remove_queue() {
    std::lock_guard<std::mutex> lock(this -> remove_mutex);
    for(socket_t& sock : remove_queue) {
        this -> sockets_type_map.erase(sock);

        if(epoll_ctl(this -> epoll_fd, EPOLL_CTL_DEL, sock, nullptr) < 0) {

        }
        
        close(sock);
    }
    remove_queue.clear();
}

void Server::modify_epoll_event(socket_t socket_fd, uint32_t new_events) {
    epoll_event ev{};
    ev.data.fd = socket_fd;
    ev.events = new_events;
    if(epoll_ctl(this -> epoll_fd, EPOLL_CTL_MOD, socket_fd, &ev) < 0) {
        std::string epoll_fail_str = SERVER_EPOLL_MOD_FAILED_ERR_MSG;
        epoll_fail_str += SERVER_ERRNO_STR_PREFIX;
        epoll_fail_str += std::to_string(errno); 
        std::cout << epoll_fail_str << std::endl;
        throw std::runtime_error(epoll_fail_str);
    }
}

void Server::modify_socket_for_sending_epoll(socket_t socket_fd) {
    this -> request_epoll_mod(socket_fd, EPOLLOUT);// | EPOLLET);
}

void Server::modify_socket_for_receiving_epoll(socket_t socket_fd) {
    this -> request_epoll_mod(socket_fd, EPOLLIN);// | EPOLLET);
}

void Server::add_message_to_response_queue(socket_t socket_fd, const Server_Message& message) {
    std::unique_lock<std::shared_mutex> lock(partition_queues_mutex);
    // std::unordered_map<socket_t, std::queue<Server_Message>>::iterator it = this -> partition_queues.find(socket_fd);
    this -> partition_queues[socket_fd].push(message);
    //std::cout << "Queued response for fd=" << message.client_id << " size=" << message.bytes_to_process << std::endl;

}

void Server::prepare_socket_for_response(socket_t socket_fd, const Server_Message& serv_msg) {
    this -> add_message_to_response_queue(socket_fd, serv_msg);
    this -> modify_socket_for_sending_epoll(socket_fd);
}

void Server::prepare_socket_for_ok_response(socket_t socket_fd, bool contain_cid, protocol_id_t client_id) {
    // std::cout << "CLIENT_ID: " << client_id << std::endl;
    // std::cout << "CLIENT_ID: " << client_id << std::endl;
    Server_Message serv_msg = this -> create_ok_response(contain_cid, client_id);
    this -> add_message_to_response_queue(socket_fd, serv_msg);
    this -> modify_socket_for_sending_epoll(socket_fd);
}     

void Server::prepare_socket_for_err_response(socket_t socket_fd, bool contain_cid, protocol_id_t client_id) {
    Server_Message serv_msg = this -> create_error_response(contain_cid, client_id);    
    this -> add_message_to_response_queue(socket_fd, serv_msg);
    this -> modify_socket_for_sending_epoll(socket_fd);
}

bool Server::tactical_reload_partition(socket_t socket_fd, Server_Request& out_msg) {
       std::unique_lock<std::shared_mutex> lock(partition_queues_mutex);
       
       if (!this -> partition_queues[socket_fd].empty()) {
           out_msg = std::move(this -> partition_queues[socket_fd].front());
           this -> partition_queues[socket_fd].pop();
           return true;
       }
       
       return false;
}

// updates bytes to process also
bool Server::add_cid_tag(Server_Message& serv_msg) {
    protocol_message_len_type b_to_prcs = serv_msg.bytes_to_process + sizeof(protocol_id_t);
    serv_msg.bytes_to_process = b_to_prcs;
    b_to_prcs = protocol_msg_len_hton(b_to_prcs);
    protocol_id_t net_cid = protocol_id_hton(serv_msg.client_id);

    serv_msg.message.insert(sizeof(protocol_message_len_type), sizeof(net_cid),'\0');
    memcpy(&serv_msg.message[sizeof(protocol_message_len_type)], &net_cid, sizeof(net_cid));
    memcpy(&serv_msg.message[0], &b_to_prcs, sizeof(b_to_prcs));
    return true;
}

bool Server::remove_cid_tag(Server_Message& serv_msg) {
    serv_msg.message.erase(sizeof(protocol_message_len_type), sizeof(protocol_id_t));
    serv_msg.bytes_to_process -= sizeof(protocol_id_t);
    protocol_message_len_type net_len = protocol_msg_len_hton(serv_msg.bytes_to_process);
    memcpy(&serv_msg.message[0], &net_len, sizeof(net_len));

    return true;
}

void Server::request_epoll_mod(socket_t socket_fd, int32_t events) {
    std::unique_lock<std::shared_mutex> lock(this -> epoll_mod_mutex);
    this -> epoll_mod_map[socket_fd] |= events;
    //std::cout << "REUQESTING CHANGE" << std::endl;
    uint64_t one = 1;
    write(this -> wakeup_fd, &one, sizeof(one)); // wake up epoll_wait
    //std::cout << "WAKEUPFD IS LOADED" <<  "" << std::endl;
}

void Server::apply_epoll_mod_q() {
    //std::cout << "MODD QUEUE" << std::endl;
    std::cout << this -> epoll_mod_map.size() << std::endl;
    std::unordered_map<socket_t, int32_t> local_copy;
    {
        // Lock only while copying to minimize contention
        std::unique_lock<std::shared_mutex> lock(this->epoll_mod_mutex);
        local_copy.swap(this->epoll_mod_map);
    }

    // Apply modifications outside the lock
    for (const auto& [fd, events] : local_copy) {
        //std::cout << "MODDING" << fd << std::endl;  
        epoll_event ev{};
        ev.data.fd = fd;
        ev.events = events;

        if (epoll_ctl(this->epoll_fd, EPOLL_CTL_MOD, fd, &ev) < 0) {
            if (errno == EBADF || errno == ENOENT) {
                // fd closed or no longer valid — skip
                continue;
            }
            std::cerr << "epoll_ctl MOD failed for fd=" << fd
                      << " errno=" << errno << std::endl;
        }
    }
}
