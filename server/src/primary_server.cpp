#include "../include/primary_server.h"
#include <fcntl.h>
#include <netdb.h>
#include <sys/epoll.h>
#include <system_error>
#include <sys/eventfd.h>

/*
FIX THE BUG FOR ENQUEUING SENDING MESSAGES

*/
uint64_t msg_sent1 = 0;
uint64_t msg_recv1 = 0;

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
        this -> sockets_type_map[partition_entry.socket_fd] = Fd_Type::PARTITION;

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
    if(partition.status != Partition_Status::PARTITION_DEAD && partition.socket_fd >= 0 && this -> sockets_type_map[partition.socket_fd] == Fd_Type::PARTITION) {
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
    ev.events = EPOLLOUT;// | EPOLLET;
    std::cout << "EPOLL" << epoll_ctl(this -> epoll_fd, EPOLL_CTL_ADD, partition.socket_fd, &ev) <<  std::endl;

    this -> sockets_type_map[partition.socket_fd] = Fd_Type::PARTITION;
    return true;
}

/**
 *  TODO IN START LOOP
 *  SEPERATE CURRENT THREADS INTO 
 *  SEND DATA TO PARTITION
 *  RECEIVE DATA FROM PARTITION
 *  ADD CONTEXT TO KNOW WHATS HAPPENING
 *  PARTITIONS NEVER ASK TO CONNECT TO US, WE ALWAYS CONNECT TO THEM, SO ACCEPT A SOCKEY IS ALWAYS A CLIENT
 */

int8_t Primary_Server::start() {
    uint64_t counter = 0;

    if (listen(this -> server_fd, 255) < 0) {   
        std::string listen_failed_str(SERVER_FAILED_LISTEN_ERR_MSG);
        listen_failed_str += SERVER_ERRNO_STR_PREFIX;
        listen_failed_str += std::to_string(errno);
        throw std::runtime_error(listen_failed_str);
    }

    init_epoll();

    add_this_to_epoll();

    add_paritions_to_epoll();

    while (true) {
        std::cout << "messages recv: " << msg_recv1 << std::endl << "messages sent: " << msg_sent1 << std::endl;
        std::cout << "EPOLLWAIT" << std::endl;
        int32_t ready_fd_count = this -> server_epoll_wait();
        std::cout << "EPOLLREADY" << std::endl;
        if(ready_fd_count < 0) {
            if(errno == EINTR) {
                continue;
            }
            if(this -> verbose > 0) {
                std::cerr << SERVER_EPOLL_WAIT_FAILED_ERR_MSG << SERVER_ERRNO_STR_PREFIX << errno << std::endl;
                break;
            }
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

            Fd_Type sock_type = this -> sockets_type_map[socket_fd];
            if(ev.events & (EPOLLHUP | EPOLLERR)) {
                if (verbose > 0) {
                    std::cerr << "Client error/hang-up: fd=" << socket_fd << std::endl;
                }
                epoll_ctl(epoll_fd, EPOLL_CTL_DEL, socket_fd, nullptr);
                this -> sockets_type_map.erase(socket_fd);
                close(socket_fd);
            }

            switch(sock_type) {
                case Fd_Type::LISTENER: {
                    this -> add_client_socket_to_epoll_ctx();
                    break;
                }

                case Fd_Type::CLIENT: {
                    if(ev.events & EPOLLIN) {
                        std::cout << "CLIENT_EPOLLIN" << std::endl;
                        // read the message from the client
                        Server_Message serv_msg = this -> read_message(socket_fd);
                        if(serv_msg.message.empty()) {
                            std::cout << "msg was empty" << std::endl;
                            break;
                        }

                        ++msg_recv1;
                        // give an id to the client
                        {   
                            std::shared_lock<std::shared_mutex> lock(this -> client_id_map_mutex);
                            serv_msg.client_id = this -> client_id_map[socket_fd];
                        }

                        // get the partitions number
                        //std::cout << "here" << std::endl;
                        process_client_in(socket_fd, serv_msg);
                        this -> thread_pool.enqueue([this, socket_fd, serv_msg](){
                            this -> process_client_in(socket_fd, serv_msg);
                        });

                    }
                    if(ev.events & EPOLLOUT) {
                        std::cout << "CLIENT_EPOLLOUT" << std::endl;
                        std::cout << "CLIENT_REQUEST: " << counter++ << std::endl;
                        std::lock_guard<std::mutex> lock(this -> partial_buffer_mutex);
                        std::unordered_map<socket_t, Server_Response>::iterator msg = this -> partial_write_buffers.find(socket_fd);
                        
                        if(msg != this -> partial_write_buffers.end()) {
                            Server_Request& data = msg -> second;
                            int64_t bytes_sent = this -> send_message(socket_fd, data);

                            std::cout << "sending_to: " << data.client_id << "FD: " << socket_fd << std::endl;
                            std::cout << bytes_sent << "/" << data.bytes_to_process << std::endl;
                            std::cout << bytes_sent << "/" << data.message.size() << std::endl;

                            for(int i = 0; i < bytes_sent; ++i) {
                                std::cout << int(data.message[i]) << " ";
                            }

                            std::cout << std::endl;

                            if(bytes_sent < 0) {
                                this -> request_to_remove_fd(socket_fd);
                                continue;
                            }

                            if(msg -> second.bytes_processed >= msg -> second.bytes_to_process) {
                                ++msg_sent1;
                                this -> partial_write_buffers.erase(msg);
                                // return to listening stage
                                try {
                                    // this -> request_epoll_mod(socket_fd, EPOLLIN | EPOLLET);
                                    this -> modify_socket_for_receiving_epoll(socket_fd);
                                }
                                catch(const std::exception& e) {
                                    if(this -> verbose > 0) {
                                        std::cerr << e.what() << std::endl;
                                    }
                                }
                                
                                continue;
                            }
                        }
                    }
                    else {
                        // figure this one client prob disconected
                    }
                    break;
                }

                case Fd_Type::PARTITION: {
                    std::cout << "PARTITION_EPOLLIN" << std::endl;
                    if(ev.events & EPOLLIN) {
                        // read the message
                        Server_Message serv_msg = this -> read_message(socket_fd);
                        if(serv_msg.message.empty()) {
                            break;
                        }

                        // get the client id and set it up to EPOLLOUT
                        socket_t client_fd;
                        {
                            std::shared_lock<std::shared_mutex> lock(id_client_map_mutex);
                            client_fd = this -> id_client_map[serv_msg.client_id];
                        }

                        this -> remove_cid_tag(serv_msg);
                        //this -> process_partition_in(client_fd, serv_msg);

                        this -> thread_pool.enqueue([this, client_fd, serv_msg]() {
                            this -> process_partition_in(client_fd, serv_msg);
                        });
                    }

                    if (ev.events & EPOLLOUT) {
                        std::cout << "PARTITION_EPOLLOUT" << std::endl;
                        
                        // Acquire lock to check/reload buffer
                        std::unique_lock<std::mutex> lock(this -> partial_buffer_mutex);
                        auto it = this -> partial_write_buffers.find(socket_fd);

                        // If no buffer exists, try to reload from queue
                        if (it == this -> partial_write_buffers.end()) {
                            Server_Request new_msg;
                            
                            // Release lock before calling tactical_reload (to avoid issues)
                            lock.unlock();
                            bool loaded = this -> tactical_reload_partition(socket_fd, new_msg);
                            
                            if (!loaded) {
                                // Nothing to write, return to EPOLLIN
                                this -> request_epoll_mod(socket_fd, EPOLLIN);//) | EPOLLET);
                                //this -> modify_socket_for_receiving_epoll(socket_fd);
                                continue;
                            }
                            
                            // Re-acquire lock to insert the new message
                            lock.lock();
                            auto insert_result = this -> partial_write_buffers.emplace(socket_fd, std::move(new_msg));
                            it = insert_result.first;
                        }

                        // NOW USE A REFERENCE - modifications will persist!
                        Server_Request& serv_req = it -> second;
                        
                        // Release lock before potentially blocking send() call
                        lock.unlock();

                        // Send data - this modifies serv_req.bytes_processed
                        int64_t bytes_sent = this -> send_message(socket_fd, serv_req);
                        
                        if (bytes_sent < 0) {
                            // Get client socket for error response
                            socket_t client_fd = -1;
                            {
                                std::shared_lock<std::shared_mutex> id_lock(this -> id_client_map_mutex);
                                auto client_it = this -> id_client_map.find(serv_req.client_id);
                                if (client_it != this -> id_client_map.end()) {
                                    client_fd = client_it -> second;
                                }
                            }

                            if (client_fd >= 0) {
                                this -> prepare_socket_for_err_response(client_fd, false, serv_req.client_id);
                            }

                            this -> request_to_remove_fd(socket_fd);
                            continue;
                        }

                        // Re-acquire lock to check if message is complete
                        lock.lock();
                        
                        // Check if entire message has been sent
                        if (serv_req.bytes_processed >= serv_req.bytes_to_process) {
                            // Message complete, remove from buffer
                            this -> partial_write_buffers.erase(socket_fd);
                            
                            // Try to load next message from queue
                            Server_Request next_msg;
                            lock.unlock();
                            
                            bool has_more = this -> tactical_reload_partition(socket_fd, next_msg);
                            
                            if (!has_more) {
                                // No more messages, switch back to EPOLLIN
                                this -> request_epoll_mod(socket_fd, EPOLLIN);//| EPOLLET);
                                // this -> modify_socket_for_receiving_epoll(socket_fd);
                            } else {
                                // More messages available, keep in EPOLLOUT mode
                                lock.lock();
                                this -> partial_write_buffers[socket_fd] = std::move(next_msg);
                                lock.unlock();
                                this -> request_epoll_mod(socket_fd, EPOLLOUT);
                                // Stay in EPOLLOUT mode - will trigger again
                            }
                        }
                        // else: partial send, stay in EPOLLOUT, will retry with updated bytes_processed
                        break;
                    }
                    else {
                        // figure this one client prob disconected
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

        this -> sockets_type_map[socket_fd] = Fd_Type::CLIENT;
    }
}

int8_t Primary_Server::process_client_in(socket_t socket_fd, const Server_Message& msg) {
    // extract the command code
    Command_Code com_code = this -> extract_command_code(msg.message, false);

    // decide how to handle it
    switch(com_code) {
        case COMMAND_CODE_GET:
        case COMMAND_CODE_SET: 
        case COMMAND_CODE_REMOVE: {
            // extract the key string
            std::string key_str;
            try {
                key_str = this -> extract_key_str_from_msg(msg.message, false);
            }
            catch(const std::exception& e) {
                if(this -> verbose) {
                    std::cerr << e.what() << std::endl;
                }
                // send a message to the client about invalid operation
                this -> prepare_socket_for_err_response(socket_fd, false, msg.client_id);
                return 0;
            }

            // find to which partition entry it belongs to
            Partition_Entry& partition_entry = this -> get_partition_for_key(key_str);
            socket_t partition_fd = partition_entry.socket_fd;

            uint64_t cid;
            {
                std::shared_lock<std::shared_mutex> lock(this -> client_id_map_mutex);
                cid = this -> client_id_map[socket_fd];
            }

            Server_Request serv_req;
            serv_req.message = msg.message;
            serv_req.bytes_to_process = msg.bytes_to_process;
            serv_req.bytes_processed = 0;
            serv_req.client_id = cid;
            this -> add_cid_tag(serv_req);

            if(!ensure_partition_connection(partition_entry)) {
                this -> prepare_socket_for_err_response(socket_fd, false, 0);
                return -1;
            }

            this -> add_message_to_response_queue(partition_fd, serv_req);

            // mark the partition as EPOLLOUT
            try {
                //this -> request_epoll_mod(partition_fd, EPOLLOUT | EPOLLET);
                this -> modify_socket_for_sending_epoll(partition_fd);
            }
            catch(const std::exception& e) {
                if(this -> verbose > 0) {
                    std::cerr << e.what() << std::endl;
                }
                this -> partitions[partition_entry.id].status = Partition_Status::PARTITION_DEAD;
                this -> prepare_socket_for_err_response(socket_fd, false, 0);
            }

            std::cout << "HERE" << std::endl;

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

    // find which partition the client belongs to
    // set that partition socket to EPOLOUT
    // quit thread
}

// create a message for the client with no id in it, 
int8_t Primary_Server::process_partition_in(socket_t socket_fd, const Server_Message& msg) {
    {
        std::unique_lock<std::mutex> lock(partial_buffer_mutex);
        partial_write_buffers[socket_fd] = msg;
    }

    try {
        this -> request_epoll_mod(socket_fd, EPOLLOUT);// | EPOLLET);
        // this -> modify_socket_for_sending_epoll(socket_fd);
    }
    catch(const std::exception& e){
        if(this -> verbose > 0) {
            std::cerr << e.what() << std::endl;
            std::cerr << "Fd = " << socket_fd << "Fd type: " << int(this ->sockets_type_map[socket_fd]) << std::endl;
        }

        return -1;
    }

    return 0;
}

void Primary_Server::add_paritions_to_epoll() {
    for(Partition_Entry& pe : this -> partitions) {
        this -> make_non_blocking(pe.socket_fd);
        epoll_event ev{};
        ev.data.fd = pe.socket_fd;
        ev.events = EPOLLIN; // | EPOLLET;
        if(epoll_ctl(this -> epoll_fd, EPOLL_CTL_ADD, pe.socket_fd, &ev) < 0) {
            std::cerr << "PARTITION NOT ADDED ERRNO " << errno << std::endl;; 
        }
    }
}