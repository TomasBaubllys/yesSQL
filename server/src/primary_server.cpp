#include "../include/primary_server.h"
#include <fcntl.h>
#include <netdb.h>
#include <sys/epoll.h>

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
        this -> sockets_map[partition_entry.socket_fd] = Socket_Types::PARTITION_SERVER_SOCKET;

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

void Primary_Server::start_partition_monitor_thread() const {
    std::thread status_thread([this]() {
        while(true) {
            this -> display_partitions_status();
            std::this_thread::sleep_for(std::chrono::seconds(PRIMARY_SERVER_PARTITION_CHECK_INTERVAL));
        }
    });
    status_thread.detach();
}

int8_t Primary_Server::start() {
    if (listen(server_fd, PRIMARY_SERVER_DEFAULT_LISTEN_VALUE) < 0) {
        std::string listen_failed_str(SERVER_FAILED_LISTEN_ERR_MSG);
        listen_failed_str += SERVER_ERRNO_STR_PREFIX;
        listen_failed_str += std::to_string(errno);
        throw std::runtime_error(listen_failed_str);
    }

    if(this -> init_epoll() < 0) {
        throw std::runtime_error(SERVER_FAILED_EPOLL_CREATE_ERR_MSG);
    }

    if (this -> add_this_to_epoll() < 0) {
        throw std::runtime_error(SERVER_FAILED_EPOLL_ADD_FAILED_ERR_MSG);
    }

    while (true) {
        int32_t ready_fd_count = this -> server_epoll_wait();
        if (ready_fd_count < 0) {
            if (errno == EINTR) continue; // interrupted by signal
            if(this -> verbose > 0) {
                std::cerr << SERVER_EPOLL_WAIT_FAILED_ERR_MSG << SERVER_ERRNO_STR_PREFIX << errno << std::endl;
            }
            break;
        }

        for (int i = 0; i < ready_fd_count; ++i) {
            file_desc_t fd = this -> epoll_events[i].data.fd;

            if (fd == server_fd) {
                socket_t client_fd = this -> add_client_socket_to_epoll(); 
                this -> sockets_map[client_fd] = Socket_Types::CLIENT_SOCKET;

            } else if (this -> epoll_events[i].events & EPOLLIN) {
                // Data is ready to be read
                try {
                    std::string message = this -> read_message(fd);
                    if (message.empty()) {
                        continue;
                    }

                    // Handle the full message
                    int8_t result = this -> handle_client_request(fd, message);
                    if (result < 0) {
                        epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fd, nullptr);
                        close(fd);
                    }
                }
                catch (const std::exception& e) {
                    if (verbose > 0)
                        std::cerr << "Error reading from fd=" << fd << ": " << e.what() << std::endl;
                    epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fd, nullptr);
                    close(fd);
                }
            } else if (this -> epoll_events[i].events & (EPOLLHUP | EPOLLERR)) {
                // Client hang-up or error
                if (verbose > 0)
                    std::cerr << "Client error/hang-up: fd=" << fd << std::endl;
                epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fd, nullptr);
                close(fd);
            }
        }
    }

    return 0;
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

int8_t Primary_Server::handle_client_request(socket_t client_socket, std::string& client_message) {
    Command_Code com_code = this -> extract_command_code(client_message);

    // decide how to handle it
    switch(com_code) {
        case COMMAND_CODE_GET:
        case COMMAND_CODE_SET: 
        case COMMAND_CODE_REMOVE: {
            // extract the key string
            std::string key_str;
            try {
                key_str = this -> extract_key_str_from_msg(client_message);
            }
            catch(const std::exception& e) {
                if(this -> verbose) {
                    std::cerr << e.what() << std::endl;
                }
                // send a message to the client about invalid operation
                this -> send_error_response(client_socket);
                return -1;
            }

            // find to which partition entry it belongs to
            Partition_Entry& partition_entry = this -> get_partition_for_key(key_str);
            std::string partition_response;
            try {
                partition_response = this -> query_partition(partition_entry, client_message);
            }
            catch(const std::exception& e) {
                this -> send_error_response(client_socket);
                break;
            }

            try{
                //int flags = fcntl(client_socket, F_GETFL, 0);
                // fcntl(client_socket, F_SETFL, flags & ~O_NONBLOCK);
                this -> send_message(client_socket, partition_response);
            }
            catch(const std::exception& e) {
                if(this -> verbose > 0) {
                    std::cerr << e.what() << std::endl;
                }
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

std::string Primary_Server::query_partition(Partition_Entry& partition, const std::string &raw_message) {
    if(!ensure_partition_connection(partition)) {
        throw std::runtime_error(PRIMARY_SERVER_FAILED_PARTITION_QUERY_ERR_MSG);
    }

    int64_t bytes_sent = this -> send_message(partition.socket_fd, raw_message);
    if(bytes_sent == 0) {
        std::string fail_str(PRIMARY_SERVER_FAILED_PARTITION_QUERY_ERR_MSG);
        fail_str += partition.name;
        close(partition.socket_fd);
        partition.status = Partition_Status::PARTITION_DEAD;
        throw std::runtime_error(fail_str);
    }

    std::string response;
    try {
        response = this -> read_message(partition.socket_fd);
        if(response.size() == 0) {
            throw std::runtime_error(PRIMARY_SERVER_FAILED_PARTITION_QUERY_ERR_MSG);
        }
    }
    catch(...) {
        close(partition.socket_fd);
        partition.status = Partition_Status::PARTITION_DEAD;
        throw;
    }

    return response;
}

bool Primary_Server::ensure_partition_connection(Partition_Entry& partition) {
    if(partition.status != Partition_Status::PARTITION_DEAD && partition.socket_fd >= 0 && this -> sockets_map[partition.socket_fd] == Socket_Types::PARTITION_SERVER_SOCKET) {
        return true;
    }

    // after reconection partition socket stays the same
    // std::cout << int(partition.status) << " " << partition.socket_fd << " " << this -> sockets_map[partition.socket_fd] << std::endl; 

    // else try to reconnect
    bool success = false;
    partition.socket_fd = this -> connect_to(partition.name, partition.port, success);
    if(!success || partition.socket_fd < 0) {
        return false;
    }

    partition.status = Partition_Status::PARTITION_FREE;
    this -> sockets_map[partition.socket_fd] = Socket_Types::PARTITION_SERVER_SOCKET;
    return true;
}
