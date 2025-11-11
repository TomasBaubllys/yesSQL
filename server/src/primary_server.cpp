#include "../include/primary_server.h"
#include <fcntl.h>
#include <netdb.h>
#include <sys/epoll.h>
#include <system_error>
#include <sys/eventfd.h>

uint64_t ttcmsgrecv = 0;
uint64_t ttcmsgsent = 0;

uint64_t pppmsgrecv = 0;
uint64_t pppmsgsent = 0;

Primary_Server::Primary_Server(uint16_t port, uint8_t verbose) : Server(port, verbose) {
    const char* partition_count_str = std::getenv(PRIMARY_SERVER_PARTITION_COUNT_ENVIROMENT_VARIABLE_STRING);
    if(!partition_count_str) {
        throw std::runtime_error(PRIMARY_SERVER_PARTITION_COUNT_ENVIROMENT_VARIABLE_UNDEF_ERR_MSG);
    }

    this -> partition_count = atoi(partition_count_str);
    if(this -> partition_count == 0) {
        throw std::runtime_error(PRIMARY_SERVER_PARTITION_COUNT_ZERO_ERR_MSG);
    }

    this -> partition_range_length = std::numeric_limits<uint32_t>::max() / this -> partition_count;

    this -> partitions.reserve(partition_count);

    for(uint32_t i = 0; i < this -> partition_count; ++i) {
        Partition_Entry partition_entry;
        partition_entry.name = PARTITION_SERVER_NAME_PREFIX + std::to_string(i + 1);
        partition_entry.port = PARTITION_SERVER_PORT;

        bool success = false;
        partition_entry.socket_fd = this -> connect_to(partition_entry.name, partition_entry.port, success);
        // check this part
        this -> fd_type_map[partition_entry.socket_fd] = Fd_Type::PARTITION;

        if(!success) {
            partition_entry.status = Partition_Status::PARTITION_DEAD;
        }
        else {
            partition_entry.status = Partition_Status::PARTITION_FREE;
        }
        
        partition_entry.range.beg = this -> partition_range_length * i;
        partition_entry.range.end = this ->  partition_range_length * (i + 1);

        // for the last partition is everything until the max index
        if(i == this -> partition_count - 1) {
            partition_entry.range.end = std::numeric_limits<uint32_t>::max();
        }

        partition_entry.id = i;

        this -> partitions.push_back(partition_entry);
    }
}

void Primary_Server::start_partition_monitor_thread() const
{
    std::thread status_thread([this]() {
        while(true) {
            this -> display_partitions_status();
            std::this_thread::sleep_for(std::chrono::seconds(PRIMARY_SERVER_PARTITION_CHECK_INTERVAL));
        }
    });
    status_thread.detach();
}

void Primary_Server::display_partitions_status() const {
    std::vector<bool> status = this -> get_partitions_status();

    for(uint32_t i = 0; i < status.size(); ++i) {
        std::cout << this -> partitions.at(i).name << " " << (status.at(i)? PARTITION_ALIVE_MSG : PARTITION_DEAD_MSG) << std::endl;
    }
}

std::vector<bool> Primary_Server::get_partitions_status() const {
    std::vector<bool> partitions_status;
    partitions_status.reserve(this -> partition_count);

    for(uint32_t i = 0; i < this -> partition_count; ++i) {
        partitions_status.push_back(try_connect(this -> partitions.at(i).name, this -> partitions.at(i).port));
    }

    return partitions_status;
}

uint32_t Primary_Server::key_prefix_to_uint32(const std::string& key) const {
    uint32_t uint32_prefix_key = 0;

    uint32_t key_limit = std::min<uint32_t>(PRIMARY_SERVER_BYTES_IN_KEY_PREFIX, key.length());

    for(uint32_t i = 0; i < key_limit; ++i) {
        uint32_prefix_key |= static_cast<uint32_t>(static_cast<uint8_t>(key[i]) << (8 * (PRIMARY_SERVER_BYTES_IN_KEY_PREFIX - 1 - i)));
    }

    return uint32_prefix_key;
}

Partition_Entry& Primary_Server::get_partition_for_key(const std::string& key) {
    uint32_t uint32_key_prefix = this -> key_prefix_to_uint32(key);

    uint32_t partition_index = static_cast<uint32_t>(uint32_key_prefix / this -> partition_range_length);

    if(partition_index >= this -> partitions.size()) {
        --partition_index;
    }

    std::shared_lock<std::shared_mutex> lock(this -> partitions_mutex);
    return this -> partitions.at(partition_index);
}

std::vector<Partition_Entry> Primary_Server::get_partitions_ff(const std::string& key) const {
    return std::vector<Partition_Entry>();
}

std::vector<Partition_Entry> Primary_Server::get_partitions_fb(const std::string& key) const {
    return std::vector<Partition_Entry>();
}

// add a mutex for this
bool Primary_Server::ensure_partition_connection(Partition_Entry& partition) {
    if(partition.status != Partition_Status::PARTITION_DEAD && partition.socket_fd >= 0 && this -> fd_type_map[partition.socket_fd] == Fd_Type::PARTITION) {
        return true;
    }

    bool success = false;
    partition.socket_fd = this -> connect_to(partition.name, partition.port, success);
    if(!success || partition.socket_fd < 0) {
        return false;
    }

    epoll_event ev{};
    ev.data.fd = partition.socket_fd;
    this -> partitions[partition.id].socket_fd = partition.socket_fd;
    this -> partitions[partition.id].status = Partition_Status::PARTITION_FREE;
    ev.events = EPOLLIN | EPOLLOUT;
    if(epoll_ctl(this -> epoll_fd, EPOLL_CTL_ADD, partition.socket_fd, &ev) < 0) {
        throw std::runtime_error(SERVER_FAILED_EPOLL_ADD_FAILED_ERR_MSG);
    }

    this -> fd_type_map[partition.socket_fd] = Fd_Type::PARTITION;
    return true;
}

int8_t Primary_Server::start() {
    if (listen(this -> server_fd, 255) < 0) {   
        std::string listen_failed_str(SERVER_FAILED_LISTEN_ERR_MSG);
        listen_failed_str += SERVER_ERRNO_STR_PREFIX;
        listen_failed_str += std::to_string(errno);
        throw std::runtime_error(listen_failed_str);
    }

    init_epoll();

    add_this_to_epoll();

    add_partitions_to_epoll();

    while (true) {
        std::cout << "Received messages from clients: " << ttcmsgrecv << std::endl;
        std::cout << "Messages sent to clients: " << ttcmsgsent << std::endl;
        std::cout << "Received messages from partitions: " << pppmsgrecv << std::endl;
        std::cout << "Messages sent to partitions: " << pppmsgsent << std::endl;
        int32_t ready_fd_count = this -> server_epoll_wait();
        if(ready_fd_count < 0) {
            if(errno == EINTR) {
                continue;
            }
            std::string epoll_wait_fail_str(SERVER_EPOLL_WAIT_FAILED_ERR_MSG);
            epoll_wait_fail_str += SERVER_ERRNO_STR_PREFIX;
            epoll_wait_fail_str += std::to_string(errno);
            throw std::runtime_error(epoll_wait_fail_str);
        }

        for(int32_t i = 0; i < ready_fd_count; ++i) {
            epoll_event ev = this -> epoll_events.at(i);
            socket_t socket_fd = ev.data.fd;

            if(socket_fd == this -> wakeup_fd) {
                uint64_t dummy = 0;
                read(socket_fd, &dummy, sizeof(dummy));
                this -> apply_epoll_mod_q();
                continue;
            }

            if(ev.events & (EPOLLHUP | EPOLLERR)) {
                if (verbose > 0) {
                    std::cerr << SERVER_EPOLL_FD_HOOKUP_ERR_MSG << std::endl;
                }
                this -> request_to_remove_fd(socket_fd);
                continue;
            }

            Fd_Type sock_type = this -> fd_type_map[socket_fd];

            switch(sock_type) {
                case Fd_Type::LISTENER: {
                    this -> add_client_socket_to_epoll_ctx();
                    break;
                }

                case Fd_Type::CLIENT: {
                    if(ev.events & EPOLLIN) {
                        std::vector<Server_Message> serv_msgs = this -> read_messages(socket_fd);
                        for(Server_Message& serv_msg : serv_msgs) {
                            if(serv_msg.is_empty()) {
                                continue;
                            }
                            
                            ++ttcmsgrecv;

                            {   
                                std::shared_lock<std::shared_mutex> lock(this -> client_id_map_mutex);
                                serv_msg.add_cid(this -> client_id_map[socket_fd]);
                            }

                            this -> thread_pool.enqueue([this, socket_fd, serv_msg](){
                                this -> process_client_in(socket_fd, serv_msg);
                            });
                        }
                    }
                    else if(ev.events & EPOLLOUT) {
                        std::unique_lock<std::shared_mutex> lock(this -> write_buffers_mutex);
                        std::unordered_map<socket_t, Server_Message>::iterator msg = this -> write_buffers.find(socket_fd);
                        if(msg == this -> write_buffers.end()) {
                            this -> request_epoll_mod(socket_fd, EPOLLIN);
                            break;
                        }

                        Server_Message& data = msg -> second;
                        try {
                            int64_t bytes_sent = this -> send_message(socket_fd, data);
                        
                            if(bytes_sent < 0) {
                                this -> request_to_remove_fd(socket_fd);
                                continue;
                            }

                            ttcmsgsent++;

                            if(data.is_fully_read()) {
                                this -> write_buffers.erase(msg);
                                // return to listening stage
                                try {
                                    this -> request_epoll_mod(socket_fd, EPOLLIN);
                                }
                                catch(const std::exception& e) {
                                    if(this -> verbose > 0) {
                                        std::cerr << e.what() << std::endl;
                                    }
                                }                            
                            }
                        }
                        catch(const std::exception& e) {
                            if(this -> verbose > 0) {
                                std::cerr << e.what() << std::endl;
                            }

                            this -> request_to_remove_fd(socket_fd);
                        }
                    }
                    break;
                }

                case Fd_Type::PARTITION: {
                    if(ev.events & EPOLLIN) {
                        // read the message
                        std::vector<Server_Message> serv_msgs = this -> read_messages(socket_fd);
                        for(const Server_Message& serv_msg : serv_msgs) {
                            if(!serv_msg.is_empty()) {
                                ++pppmsgrecv;
                                this -> thread_pool.enqueue([this, serv_msg]() {
                                    this -> process_partition_response(serv_msg);
                                });
                            }
                        }
                    }

                    if (ev.events & EPOLLOUT) {                        
                        // Acquire lock to check/reload buffer
                        std::unique_lock<std::shared_mutex> lock(this -> write_buffers_mutex);
                        std::unordered_map<socket_t, Server_Message>::iterator it = this -> write_buffers.find(socket_fd);

                        if (it == this -> write_buffers.end()) {
                            Server_Message new_msg;
                            
                            bool loaded = this -> tactical_reload_partition(socket_fd, new_msg);
                            
                            if (!loaded) {
                                this -> request_epoll_mod(socket_fd, EPOLLIN);//) | EPOLLET);
                                break;
                            }
                            
                            std::pair<std::unordered_map<socket_t, Server_Message>::iterator, bool> insert_result = this -> write_buffers.emplace(socket_fd, std::move(new_msg));
                            if(insert_result.second) {
                                it = insert_result.first;
                            }
                            else {
                                throw std::runtime_error(SERVER_FAILED_PARTITION_WRITE_BUFFER_ERR_MSG);
                            }
                        }

                        Server_Message& serv_req = it -> second;                        
                        
                        try {
                            int64_t bytes_sent = this -> send_message(socket_fd, serv_req);
                        
                            if (bytes_sent < 0) {
                                // Get client socket for error response
                                socket_t client_fd = -1;
                                {
                                    std::shared_lock<std::shared_mutex> id_lock(this -> id_client_map_mutex);
                                    std::unordered_map<protocol_id_t, socket_t>::iterator client_it = this -> id_client_map.find(serv_req.get_cid());
                                    if (client_it != this -> id_client_map.end()) {
                                        client_fd = client_it -> second;
                                    }
                                }

                                if (client_fd >= 0) {
                                    this -> queue_client_for_error_response(client_fd);
                                }

                                this -> request_to_remove_fd(socket_fd);
                                lock.unlock();
                                break;
                            }
                        }
                        catch(const std::exception& e) {
                            if(this -> verbose > 0) {
                                std::cerr << e.what() << std::endl;
                            }
                            this -> request_to_remove_fd(socket_fd);
                        }
                        
                        if (serv_req.is_fully_read()) {
                            ++pppmsgsent;
                            std::cout << "FD = " << socket_fd << " sent to parition: " << std::endl;
                            serv_req.print();
                            this -> write_buffers.erase(socket_fd);
                            
                            Server_Message next_msg;
                            
                            bool has_more = this -> tactical_reload_partition(socket_fd, next_msg);
                            if (!has_more) {
                                this -> request_epoll_mod(socket_fd, EPOLLIN | EPOLLOUT);//| EPOLLET);
                            } else {
                                this -> write_buffers[socket_fd] = std::move(next_msg);
                            }
                        }
                        lock.unlock();
                    }

                    break;
                }
                default: {
                    break;
                }
            }

        }
        this -> process_remove_queue();
    }

    return 0;
}

void Primary_Server::add_client_socket_to_epoll_ctx() {
    std::vector<socket_t> client_fds = this -> add_client_socket_to_epoll();

    for(socket_t& socket_fd : client_fds) {
        uint64_t cid = this -> req_id.fetch_add(1, std::memory_order_relaxed);
        {
            std::unique_lock<std::shared_mutex> lock(this -> id_client_map_mutex);
            this -> id_client_map[cid] = socket_fd;
        }

        {
            std::unique_lock<std::shared_mutex> lock(this -> client_id_map_mutex);
            this -> client_id_map[socket_fd] = cid; 
        }

        this -> fd_type_map[socket_fd] = Fd_Type::CLIENT;
    }
}

int8_t Primary_Server::process_client_in(socket_t client_fd, Server_Message msg) {
    Command_Code com_code = this -> extract_command_code(msg.string(), true);
    switch(com_code) {
        case COMMAND_CODE_GET:
        case COMMAND_CODE_SET: 
        case COMMAND_CODE_REMOVE: {
            // extract the key string
            std::string key_str;
            try {
                key_str = this -> extract_key_str_from_msg(msg.string(), true);
            }
            catch(const std::exception& e) {
                if(this -> verbose) {
                    std::cerr << e.what() << std::endl;
                }
                this -> queue_client_for_error_response(client_fd);
                return 0;
            }

            // find to which partition entry it belongs to
            Partition_Entry& partition_entry = this -> get_partition_for_key(key_str);
            socket_t partition_fd = partition_entry.socket_fd;

            msg.reset_processed();

            if(!ensure_partition_connection(partition_entry)) {
                this -> queue_client_for_error_response(client_fd);
                return 0;
            }

            try {
                this -> queue_socket_for_response(partition_fd, msg);
            }
            catch(const std::exception& e) {
                if(this -> verbose > 0) {
                    std::cerr << e.what() << std::endl;
                }
                this -> partitions[partition_entry.id].status = Partition_Status::PARTITION_DEAD;
                this -> queue_client_for_error_response(client_fd);
            }

            break;

        }
        case COMMAND_CODE_GET_KEYS: {
            break;
        }
        case COMMAND_CODE_GET_KEYS_PREFIX: {
            break;
        }
        case COMMAND_CODE_GET_FF: {
            break;
        }
        case COMMAND_CODE_GET_FB: {
            break;
        }
        default: {

            break;
        }

    }

    return 0;
}

// create a message for the client with no id in it, 
int8_t Primary_Server::process_partition_response(Server_Message msg) {
    msg.remove_cid();
    msg.reset_processed();

    socket_t client_fd;
    {
        std::shared_lock<std::shared_mutex> lock(id_client_map_mutex);
        client_fd = this -> id_client_map[msg.get_cid()];
        /*
        * check if its still the same client!!!!!
        *
        */
    }
    
    {
        std::unique_lock<std::shared_mutex> lock(this -> write_buffers_mutex);
        write_buffers[client_fd] = std::move(msg);
    }

    try {
        this -> request_epoll_mod(client_fd, EPOLLOUT); // | EPOLLET);
    }
    catch(const std::exception& e){
        if(this -> verbose > 0) {
            std::cerr << e.what() << std::endl;
        }

        this -> request_to_remove_fd(client_fd);

        return -1;
    }

    return 0;
}

void Primary_Server::add_partitions_to_epoll() {
    for(Partition_Entry& pe : this -> partitions) {
        this -> make_non_blocking(pe.socket_fd);
        epoll_event ev{};
        ev.data.fd = pe.socket_fd;
        ev.events = EPOLLIN; // | EPOLLET;
        if(epoll_ctl(this -> epoll_fd, EPOLL_CTL_ADD, pe.socket_fd, &ev) < 0) {
            std::string pe_epoll_str(PRIMARY_SERVER_FAILED_PARTITION_ADD_ERR_MSG);
            pe_epoll_str += SERVER_ERRNO_STR_PREFIX;
            pe_epoll_str += std::to_string(errno);
            throw std::runtime_error(pe_epoll_str);
        }
    }
}

void Primary_Server::queue_client_for_error_response(socket_t client_fd) {
    // later a check if its still the same client
    Server_Message err_response = this -> create_error_response(false, client_fd);
    {
        std::unique_lock<std::shared_mutex> lock(this -> write_buffers_mutex);
        this -> write_buffers[client_fd] = err_response;
    }
    this -> request_epoll_mod(client_fd, EPOLLOUT);
}

void Primary_Server::queue_client_for_ok_response(socket_t client_fd) {
    // later a check if its still the same client
    Server_Message ok_response = this -> create_ok_response(false, client_fd);
    {
        std::unique_lock<std::shared_mutex> lock(this -> write_buffers_mutex);
        this -> write_buffers[client_fd] = ok_response;
    }
    this -> request_epoll_mod(client_fd, EPOLLOUT);
}