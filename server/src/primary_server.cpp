#include "../include/primary_server.h"
#include <fcntl.h>
#include <netdb.h>
#include <sys/epoll.h>

/*
ADDDD MUTEX TO PARTITION ENTRIES


*/

Primary_Server::Primary_Server(uint16_t port, uint8_t verbose) : Server(port, verbose) {
    const char* partition_count_str = std::getenv(PRIMARY_SERVER_PARTITION_COUNT_ENVIROMENT_VARIABLE_STRING);
    if(!partition_count_str) {
        throw std::runtime_error(PRIMARY_SERVER_PARTITION_COUNT_ENVIROMENT_VARIABLE_UNDEF_ERR_MSG);
    }

    this -> partition_count = atoi(partition_count_str);
    if(this -> partition_count == 0) { // || this -> partition_count >= PRIMARY_SERVER_MAX_PARTITION_COUNT) {
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
        this -> sockets_type_map[partition_entry.socket_fd] = Socket_Types::PARTITION_SERVER_SOCKET;

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

bool Primary_Server::ensure_partition_connection(Partition_Entry& partition) {
    if(partition.status != Partition_Status::PARTITION_DEAD && partition.socket_fd >= 0 && this -> sockets_type_map[partition.socket_fd] == Socket_Types::PARTITION_SERVER_SOCKET) {
        return true;
    }

    bool success = false;
    partition.socket_fd = this -> connect_to(partition.name, partition.port, success);
    if(!success || partition.socket_fd < 0) {
        return false;
    }

    partition.status = Partition_Status::PARTITION_FREE;
    this -> sockets_type_map[partition.socket_fd] = Socket_Types::PARTITION_SERVER_SOCKET;
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
    if (listen(this->server_fd, 255) < 0) {   
        std::string listen_failed_str(SERVER_FAILED_LISTEN_ERR_MSG);
        listen_failed_str += SERVER_ERRNO_STR_PREFIX;
        listen_failed_str += std::to_string(errno);
        throw std::runtime_error(listen_failed_str);
    }

    init_epoll();

    add_this_to_epoll();

    while (true) {
        int32_t ready_fd_count = this -> server_epoll_wait();
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
            Fd_Context* fd_ctx = static_cast<Fd_Context*>(ev.data.ptr);
            if(ev.events & (EPOLLHUP | EPOLLERR)) {
                if (verbose > 0) {
                    std::cerr << "Client error/hang-up: fd=" << socket_fd << std::endl;
                }
                epoll_ctl(epoll_fd, EPOLL_CTL_DEL, socket_fd, nullptr);
                this -> sockets_type_map.erase(socket_fd);
                close(socket_fd);
                if(fd_ctx) {
                    delete fd_ctx;
                    fd_ctx = nullptr;
                }
            }

            switch(fd_ctx -> fd_type) {
                case Fd_Type::LISTENER: {
                    socket_t client_fd = this -> add_client_socket_to_epoll_ctx(Fd_Type::CLIENT);
                    this -> sockets_type_map[client_fd] = Socket_Types::CLIENT_SOCKET;
                    break;
                }

                case Fd_Type::CLIENT: {
                    if(ev.events & EPOLLIN) {
                        // read the message from the client
                        Server_Message serv_msg = this -> read_message(socket_fd);
                        if(serv_msg.message.empty()) {
                            continue;
                        }
                        // get the partitions number
                        this -> thread_pool.enqueue([this, socket_fd, serv_msg](){
                            this -> process_client_in(socket_fd, serv_msg);
                        });
                    }
                    else if(ev.events & EPOLLOUT) {
                       std::lock_guard<std::mutex> lock(this -> response_queue_mutex);
                        std::unordered_map<socket_t, Server_Response>::iterator msg = this -> partial_write_buffers.find(socket_fd);
                        
                        if(msg != this -> partial_write_buffers.end()) {
                            Server_Request& data = msg -> second;
                            int64_t bytes_sent = this -> send_message(socket_fd, data);
                            if(bytes_sent < 0) {
                                this -> request_to_remove_fd(socket_fd);
                                continue;
                            }

                            if(msg -> second.bytes_to_process <= msg -> second.bytes_processed) {
                                this -> partial_write_buffers.erase(msg);
                                // return to listening stage
                                try {
                                    this -> modify_socket_for_receiving_epoll(socket_fd);
                                }
                                catch(const std::exception& e) {
                                    if(this -> verbose > 0) {
                                        std::cerr << e.what() << std::endl;
                                    }
                                } 
                            }
                        }
                    }
                    else {
                        // figure this one client prob disconected
                    }
                    break;
                }

                case Fd_Type::PARTITION: {
                    if(ev.events & EPOLLIN) {
                        // read the message
                        Server_Message serv_msg = this -> read_message(socket_fd);
                        if(serv_msg.message.empty()) {
                            continue;
                        }

                        // get the client id and set it up to EPOLLOUT
                        socket_t client_fd;
                        {
                            std::shared_lock<std::shared_mutex> lock(id_client_map_mutex);
                            client_fd = this -> id_client_map[serv_msg.client_id];
                        }

                        this -> thread_pool.enqueue([this, client_fd, serv_msg]() {
                            this -> process_partition_in(client_fd, serv_msg);
                        });

                    }
                    else if (ev.events & EPOLLOUT) {
                        Server_Request serv_req;
                        bool has_data = false;
                        {

                            std::lock_guard<std::mutex> lock(this -> response_queue_mutex);
                            std::unordered_map<socket_t, Server_Request>::iterator it = this -> partial_write_buffers.find(socket_fd);

                            if (it == this -> partial_write_buffers.end()) {
                                // Try to reload a pending request
                                bool loaded = this -> tactical_reload_partition(socket_fd);
                                if (!loaded) {
                                    // Nothing to write, return to EPOLLIN
                                    this->modify_socket_for_receiving_epoll(socket_fd);
                                    continue;
                                }
                                // reload created a buffer entry, refetch iterator
                                it = this->partial_write_buffers.find(socket_fd);
                                if (it != this -> partial_write_buffers.end()) {
                                    serv_req = it -> second;
                                    has_data = true;
                                }
                            }
                        }
                        if(has_data) {
                            int64_t bytes_sent = this -> send_message(socket_fd, serv_req);
                            if (bytes_sent < 0) {
                                this -> request_to_remove_fd(socket_fd);
                                continue;
                            }

                            if (serv_req.bytes_processed >= serv_req.bytes_to_process) {

                                {
                                    std::unique_lock<std::mutex> lock(response_queue_mutex);
                                    this -> partial_write_buffers.erase(socket_fd);
                                }
                                this -> modify_socket_for_receiving_epoll(socket_fd);
                            }
                        }
                    }

                    else {
                        // figure this one client prob disconected
                    }

                    break;
                }
                default:
                    break;
            }

        }

        this -> process_remove_queue();
    }

    return 0;
}

socket_t Primary_Server::add_client_socket_to_epoll_ctx(Fd_Type fd_type) {
    Fd_Context* fd_ctx = new Fd_Context;
    fd_ctx -> fd_type = fd_type;
    socket_t client_fd = this -> add_client_socket_to_epoll(fd_ctx);
    uint64_t cid = this -> req_id.fetch_add(1, std::memory_order_relaxed);
    {
        std::unique_lock<std::shared_mutex> lock(this -> id_client_map_mutex);
        this -> id_client_map[cid] = client_fd;
    }

    {
        std::unique_lock<std::shared_mutex> lock(this -> client_id_map_mutex);
        this -> client_id_map[client_fd] = cid; 
    }

    return client_fd;
}

int8_t Primary_Server::process_client_in(socket_t socket_fd, const Server_Message& msg) {
    // extract the command code
    Command_Code com_code = this -> extract_command_code(msg.message);

    // decide how to handle it
    switch(com_code) {
        case COMMAND_CODE_GET:
        case COMMAND_CODE_SET: 
        case COMMAND_CODE_REMOVE: {
            // extract the key string
            std::string key_str;
            try {
                key_str = this -> extract_key_str_from_msg(msg.message);
            }
            catch(const std::exception& e) {
                if(this -> verbose) {
                    std::cerr << e.what() << std::endl;
                }
                // send a message to the client about invalid operation
                this -> prepare_socket_for_err_response(socket_fd, msg.client_id);
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
            serv_req.bytes_processed = 0;
            serv_req.client_id = cid;
            serv_req.bytes_to_process = msg.message.size();

            // add a new Request contex and link client with this partition
            Query_Context q_ctx;
            q_ctx.client_socket = socket_fd;
            q_ctx.partition_sockets.push_back(partition_entry.socket_fd);
            q_ctx.message = serv_req;
            q_ctx.query_direction = Query_Direction::CLIENT_TO_PARTITION;
            q_ctx.client_id = cid;

            bool was_empty = false;
            // add the request id to the partition queues
            {
                std::unique_lock<std::shared_mutex> lock(partition_queues_mutex);
                was_empty = partition_queues[partition_fd].empty();
                partition_queues[partition_fd].push(cid);
            }

            // since next time we get this request its in reverse
            {
                std::unique_lock<std::shared_mutex> lock(query_map_mutex);
                this -> query_map[q_ctx.client_id] = q_ctx;
            }

            if(was_empty) {
                std::unique_lock<std::mutex> lock(response_queue_mutex);
                this -> partial_write_buffers[q_ctx.partition_sockets.front()] = serv_req;
            }

            // mark the partition as EPOLLOUT
            this -> modify_socket_for_sending_epoll(partition_entry.socket_fd);
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
        std::unique_lock<std::mutex> lock(response_queue_mutex);
        partial_write_buffers[socket_fd] = msg;
    }

    this -> modify_socket_for_sending_epoll(socket_fd);

}

bool Primary_Server::tactical_reload_partition(socket_t socket_fd) {
    uint64_t cid = 0;
    bool loaded = false;
    {
        std::unique_lock<std::shared_mutex> lock(partition_queues_mutex);
        if (!this -> partition_queues[socket_fd].empty()) {
            cid = this->partition_queues[socket_fd].front();
            this -> partition_queues[socket_fd].pop();
            loaded = true;
        }
    }

    if (loaded) {
        std::shared_lock<std::shared_mutex> lock_map(query_map_mutex);
        std::unordered_map<uint64_t, Query_Context>::iterator it = this -> query_map.find(cid);
        if (it != this->query_map.end()) {
            std::lock_guard<std::mutex> resp_lock(response_queue_mutex);
            this -> partial_write_buffers[socket_fd] = std::move(it -> second.message);
        } else {
            loaded = false;
        }
    }

    return loaded;
}
