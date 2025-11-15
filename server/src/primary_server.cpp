#include "../include/primary_server.h"
#include <cstdint>
#include <cstring>
#include <fcntl.h>
#include <netdb.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>

Primary_Server::Primary_Server(uint16_t port, uint8_t verbose, uint32_t thread_pool_size) : Server(port, verbose, thread_pool_size) {
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

Primary_Server::~Primary_Server() {

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

Partition_Entry Primary_Server::get_partition_for_key(const std::string& key) {
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

    std::unique_lock<std::shared_mutex> lock(this -> partitions_mutex);
    bool success = false;
    partition.socket_fd = this -> connect_to(partition.name, partition.port, success);
    if(!success || partition.socket_fd < 0) {
        this -> partitions[partition.id].status = Partition_Status::PARTITION_DEAD;
        return false;
    }

    epoll_event ev{};
    this -> make_non_blocking(partition.socket_fd);
    ev.data.fd = partition.socket_fd;
    this -> partitions[partition.id].socket_fd = partition.socket_fd;
    this -> partitions[partition.id].status = Partition_Status::PARTITION_FREE;
    ev.events = EPOLLIN;
    if(epoll_ctl(this -> epoll_fd, EPOLL_CTL_ADD, partition.socket_fd, &ev) < 0) {
        throw std::runtime_error(SERVER_FAILED_EPOLL_ADD_FAILED_ERR_MSG);
    }

    this -> fd_type_map[partition.socket_fd] = Fd_Type::PARTITION;
    return true;
}

int8_t Primary_Server::start() {
    if (listen(this -> server_fd, SOMAXCONN) < 0) {   
        std::string listen_failed_str(SERVER_FAILED_LISTEN_ERR_MSG);
        listen_failed_str += SERVER_ERRNO_STR_PREFIX;
        listen_failed_str += std::to_string(errno);
        throw std::runtime_error(listen_failed_str);
    }

    init_epoll();

    add_this_to_epoll();

    add_partitions_to_epoll();

    while (true) {
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
                        std::vector<Server_Message> serv_msgs;
                        try {
                            serv_msgs = this -> read_messages(socket_fd);
                        }
                        catch(const std::exception& e) {
                            if(this -> verbose > 0) {
                                std::cerr << e.what() << std::endl;
                            }
                            this -> remove_client(socket_fd);
                            break;
                        }

                        for(Server_Message& serv_msg : serv_msgs) {
                            if(serv_msg.is_empty()) {
                                continue;
                            }
                            
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
                        if(this -> is_still_same_client(data.get_cid()) < 0) {
                            this -> write_buffers.erase(socket_fd);
                            this ->request_epoll_mod(socket_fd, EPOLLIN);
                            break;
                        }

                        try {
                            this -> send_message(socket_fd, data);

                            if(data.is_fully_read()) {
                                this -> write_buffers.erase(msg);
                                // return to listening stage
                                
                                this -> request_epoll_mod(socket_fd, EPOLLIN);                          
                            }
                        }
                        catch(const std::exception& e) {
                            if(this -> verbose > 0) {
                                std::cerr << e.what() << std::endl;
                            }
                            lock.unlock();
                            this -> remove_client(socket_fd);
                        }
                    }
                    break;
                }

                case Fd_Type::PARTITION: {
                    if(ev.events & EPOLLIN) {
                        // read the message
                        std::vector<Server_Message> serv_msgs = this -> read_messages(socket_fd);
                        for(Server_Message& serv_msg : serv_msgs) {
                            if(!serv_msg.is_empty()) {
                                this -> thread_pool.enqueue([this, moved_msg = std::move(serv_msg)]() mutable {
                                    this -> process_partition_response(std::move(moved_msg));
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
                            this -> send_message(socket_fd, serv_req);
                        }
                        catch(const std::exception& e) {
                            if(this -> verbose > 0) {
                                std::cerr << e.what() << std::endl;
                            }
                            this -> request_to_remove_fd(socket_fd);
        
                            socket_t client_fd = -1;

                            {
                                std::shared_lock<std::shared_mutex> id_lock(this -> id_client_map_mutex);
                                std::unordered_map<protocol_id_t, socket_t>::iterator client_it = this -> id_client_map.find(serv_req.get_cid());
                                if (client_it != this -> id_client_map.end()) {
                                    client_fd = client_it -> second;
                                }
                            }

                            if (client_fd >= 0) {
                                this -> queue_client_for_error_response(client_fd, serv_req.get_cid());
                            }

                            lock.unlock();
                            break;
                        }
                        
                        if (serv_req.is_fully_read()) {
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
                this -> queue_client_for_error_response(client_fd, msg.get_cid());
                return 0;
            }

            // find to which partition entry it belongs to
            Partition_Entry partition_entry = this -> get_partition_for_key(key_str);
            socket_t partition_fd = partition_entry.socket_fd;

            msg.reset_processed();

            if(!ensure_partition_connection(partition_entry)) {
                this -> queue_client_for_error_response(client_fd, msg.get_cid());
                return 0;
            }

            try {
                this -> queue_partition_for_response(partition_fd, std::move(msg));
            }
            catch(const std::exception& e) {
                if(this -> verbose > 0) {
                    std::cerr << e.what() << std::endl;
                }
                this -> partitions[partition_entry.id].status = Partition_Status::PARTITION_DEAD;
                this -> queue_client_for_error_response(client_fd, msg.get_cid());
            }

            break;
        }
        case CREATE_CURSOR: {
            // msg.print();
            Cursor cursor;
            try {
                cursor = this -> extract_cursor_creation(msg);
                Partition_Entry p_id = this -> get_partition_for_key(cursor.get_next_key());
                cursor.set_last_called_part_id(p_id.id);
            } catch (const std::exception& e) {
                if(this -> verbose > 0) {
                    std::cerr << e.what() << std::endl;
                }
                this -> queue_client_for_error_response(client_fd, msg.get_cid());
                return 0;
            }
            
            {
                std::unique_lock<std::shared_mutex> lock(this -> client_cursor_map_mutex);
                this -> client_cursor_map[client_fd][cursor.get_name()] = std::move(cursor);
                // cursor.print(); 
            }

            this -> queue_client_for_ok_response(client_fd, msg.get_cid());
            break;
        }
        case DELETE_CURSOR: {
            std::string cursor_name;
            try {
                cursor_name = this -> extract_cursor_name(msg);
            }
            catch(const std::exception& e) {
                if(this -> verbose > 0) {
                    std::cerr << e.what() << std::endl;
                }

                this -> queue_client_for_error_response(client_fd, msg.get_cid());
            }

            {
                std::shared_lock<std::shared_mutex> lock(this -> client_cursor_map_mutex);
                std::unordered_map<socket_t, std::unordered_map<std::string, Cursor>>::iterator c_c_it = this -> client_cursor_map.find(client_fd);
                if(c_c_it -> second.erase(cursor_name) > 0) {
                    this -> queue_client_for_ok_response(client_fd, msg.get_cid());
                    
                }
                else {
                    this -> queue_client_for_error_response(client_fd, msg.get_cid());
                }
            }

            break;
        }
        case COMMAND_CODE_GET_FF:
        case COMMAND_CODE_GET_FB:
        case COMMAND_CODE_GET_KEYS: {
            try {
            // extract the cursors name and find it
                std::pair<std::string, cursor_cap_t> name_cap = this -> extract_cursor_name_cap(msg);
                Cursor cursor = this -> locate_cursor(client_fd, name_cap.first);
                cursor.set_capacity(name_cap.second);
                this -> query_partition_by_cursor(cursor, com_code, msg.get_cid(), false);
                this -> return_cursor(client_fd, std::move(cursor));
            }
            catch(const std::exception& e) {
                if(this -> verbose > 0) {
                    std::cerr << e.what() << std::endl;
                }

                this -> queue_client_for_error_response(client_fd, msg.get_cid());
            }
            break;
        }
        case COMMAND_CODE_GET_KEYS_PREFIX: {
            break;
        }
        default: {
            this -> queue_client_for_error_response(client_fd, msg.get_cid());
            break;
        }

    }

    return 0;
}

// create a message for the client with no id in it, 
int8_t Primary_Server::process_partition_response(Server_Message&& msg) {
    msg.reset_processed();

    // open the message check for command code, if the command code is not get_ff, get_fb, get_keys, COMMAND_CODE_GET_KEYS_PREFIX
    Command_Code com_code = this -> extract_command_code(msg.get_string_data(), true);

    switch(com_code) {
        case Command_Code::COMMAND_CODE_GET_FB:
        case Command_Code::COMMAND_CODE_GET_FF: {
            // check how many elements were returned
            // protocol_array_len_t el_returned_tmp = this -> extract_array_size(msg.get_string_data(), true);
            // extract all the entries returned, the next_key, and the cursor name
            // cursor_cap_t el_returned = static_cast<cursor_cap_t>(el_returned_tmp);

            Cursor_Info curs_inf;
            std::string next_key_str;
            std::vector<Entry> entries = this -> extract_got_entries_and_info(msg, curs_inf, next_key_str);

            // based on the given info try to find a cursor
            // 1) find the client_fd
            socket_t client_fd = this -> find_client_fd(msg.get_cid());
            if(client_fd < 0) {
                return -1;
            }

            // step 2 locate the clients cursor 
            Cursor cursor;
            try {
                cursor = this -> locate_cursor(client_fd, curs_inf.name);
            }
            catch(const std::exception& e) {
                if(this -> verbose > 0) {
                    std::cerr << e.what() << std::endl;
                }
                this -> queue_client_for_error_response(client_fd, msg.get_cid());
                return -1;
            }

            // might be a little unsafe cast but who cares
            cursor.set_next_key(next_key_str, next_key_str.size());
            cursor.add_new_entries(std::move(entries));

            // else cursor is not complete, find to which partition we
            uint16_t partition_count = 0;
            {
                std::shared_lock<std::shared_mutex> lock(this -> partitions_mutex);
                partition_count = this -> partitions.size();
            }

            if(cursor.is_complete()) {
                if(com_code == Command_Code::COMMAND_CODE_GET_FF && next_key_str == ENTRY_PLACEHOLDER_KEY) {
                    if(cursor.get_last_called_part_id() == partition_count - 1) {
                        // HOTFIX
                        protocol_key_len_t next_key_lent = UINT16_MAX;
                        std::string bin(UINT16_MAX, '\xFF');
                        cursor.set_next_key(std::move(bin), next_key_lent);
                    }
                    else {
                        protocol_key_len_t next_key_len = 0;
                        cursor.set_next_key(std::move(""), next_key_len);
                        cursor.incr_pid();
                    }
                }
                else if(com_code == Command_Code::COMMAND_CODE_GET_FB && next_key_str == ENTRY_PLACEHOLDER_KEY) {
                    if(cursor.get_last_called_part_id() == 0) {
                        // HOTFIX
                        protocol_key_len_t next_key_lent = 0;
                        std::string bin = "";
                        cursor.set_next_key(std::move(bin), next_key_lent);
                    }
                    else {
                        protocol_key_len_t next_key_lent = UINT16_MAX;
                        std::string bin(UINT16_MAX, '\xFF');
                        cursor.set_next_key(std::move(bin), next_key_lent);
                        cursor.decr_pid();
                    }
                }


                std::string resp_str = this -> create_entries_response(cursor.get_entries(), false, msg.get_cid());
                Server_Message serv_resp;
                serv_resp.set_message_eat(std::move(resp_str));
                serv_resp.set_cid(msg.get_cid());
 
                try {
                    cursor.clear_msg();
                    this -> return_cursor(client_fd, std::move(cursor));
                }
                catch(const std::exception& e) {
                    if(this -> verbose > 0) {
                        std::cerr << e.what() << std::endl;
                    }
                    this -> queue_client_for_error_response(client_fd, msg.get_cid());
                    return 0;
                }

                this -> queue_client_for_response(std::move(serv_resp));
                return 0;
            }

            if(com_code == Command_Code::COMMAND_CODE_GET_FF) {
                // no more partitions to queue
                if(cursor.get_next_key() == ENTRY_PLACEHOLDER_KEY) {
                    if(cursor.get_last_called_part_id() == uint16_t(partition_count - 1)) {
                        // no more entries available
                        std::string resp_str = this -> create_entries_response(cursor.get_entries(), false, msg.get_cid());
                        Server_Message serv_resp;
                        serv_resp.set_message_eat(std::move(resp_str));
                        serv_resp.set_cid(msg.get_cid()); 
                        cursor.clear_msg();
                        this -> return_cursor(client_fd, std::move(cursor));
                        this -> queue_client_for_response(std::move(serv_resp));
                        return 0;
                    }
                    else {
                        cursor.incr_pid();
                        // WHEN LOOKING FOR BUGS CHECK HERE!!!!
                        std::string empty_str = "";
                        cursor.set_next_key(empty_str, 0);
                        this -> query_partition_by_cursor(cursor, com_code, msg.get_cid(), false);
                        this -> return_cursor(client_fd, std::move(cursor));
                        return 0;
                    }
                }
                else {
                    this -> query_partition_by_cursor(cursor, com_code, msg.get_cid(), false);
                    this -> return_cursor(client_fd, std::move(cursor));
                    return 0;
                }
            }
            else if(com_code == Command_Code::COMMAND_CODE_GET_FB) {
                // no more partitions to queue
                if(cursor.get_next_key() == ENTRY_PLACEHOLDER_KEY) {
                    if(cursor.get_last_called_part_id() == 0) {
                        std::string resp_str = this -> create_entries_response(cursor.get_entries(), false, msg.get_cid());
                        Server_Message serv_resp;
                        serv_resp.set_message_eat(std::move(resp_str));
                        serv_resp.set_cid(msg.get_cid()); 
                        cursor.clear_msg();
                        this -> return_cursor(client_fd, std::move(cursor));
                        this -> queue_client_for_response(std::move(serv_resp));
                        return 0;

                    }
                    else {
                        cursor.decr_pid();
                        this -> query_partition_by_cursor(cursor, com_code, msg.get_cid(), true);
                        this -> return_cursor(client_fd, std::move(cursor));
                        return 0;
                    }
                }
                else {
                    this -> query_partition_by_cursor(cursor, com_code, msg.get_cid(), false);
                    this -> return_cursor(client_fd, std::move(cursor));
                    return 0;
                }
            }
        }
        case Command_Code::COMMAND_CODE_GET_KEYS: {
            // check how many elements were returned
            // protocol_array_len_t el_returned_tmp = this -> extract_array_size(msg.get_string_data(), true);
            // extract all the entries returned, the next_key, and the cursor name
            // cursor_cap_t el_returned = static_cast<cursor_cap_t>(el_returned_tmp);

            // if enough elements, reconstruct a client message with OK and elements
            // locate the cursor.... WHAT CURSOR YOU ASK????
            // if(el_returned >= )

            // if not enough either query the forward or the backward partitions



            break;
        }

        case Command_Code::COMMAND_CODE_GET_KEYS_PREFIX : {

            break;
        }

        default: {
            try {
                msg.remove_cid();
                this -> queue_client_for_response(std::move(msg));
            }
            catch(const std::exception& e){
                if(this -> verbose > 0) {
                    std::cerr << e.what() << std::endl;
                }

                return -1;
            }
        }
    }

    return 0;
}

void Primary_Server::queue_client_for_response(Server_Message &&msg) {
    socket_t client_fd;
    {
        std::shared_lock<std::shared_mutex> lock(id_client_map_mutex);
        client_fd = this -> id_client_map[msg.get_cid()];
    }

    {
        std::unique_lock<std::shared_mutex> lock(this -> write_buffers_mutex);
        if(client_fd >= 0) {
            write_buffers[client_fd] = std::move(msg);
        }

    }

    this -> request_epoll_mod(client_fd, EPOLLOUT); // | EPOLLET);
}

void Primary_Server::add_partitions_to_epoll() {
    for(Partition_Entry& pe : this -> partitions) {
        this -> make_non_blocking(pe.socket_fd);
        epoll_event ev{};
        ev.data.fd = pe.socket_fd;
        ev.events = EPOLLIN; // | EPOLLET;
        if(epoll_ctl(this -> epoll_fd, EPOLL_CTL_ADD, pe.socket_fd, &ev) < 0) {
            if(this -> verbose > 0) {
                std::cerr << PRIMARY_SERVER_FAILED_PARTITION_ADD_ERR_MSG << SERVER_ERRNO_STR_PREFIX << errno << std::endl;
           }
        }
    }
}

void Primary_Server::queue_client_for_error_response(socket_t client_fd, protocol_id_t client_id) {
    // later a check if its still the same client
    Server_Message err_response = this -> create_error_response(false, client_id);
    {
        std::unique_lock<std::shared_mutex> lock(this -> write_buffers_mutex);
        this -> write_buffers[client_fd] = err_response;
    }
    this -> request_epoll_mod(client_fd, EPOLLOUT);
}

void Primary_Server::queue_client_for_ok_response(socket_t client_fd, protocol_id_t client_id) {
    // later a check if its still the same client
    Server_Message ok_response = this -> create_ok_response(false, client_id);
    {
        std::unique_lock<std::shared_mutex> lock(this -> write_buffers_mutex);
        this -> write_buffers[client_fd] = ok_response;
    }
    this -> request_epoll_mod(client_fd, EPOLLOUT);
}

void Primary_Server::process_remove_queue() {
    std::vector<socket_t> to_remove;
    {
        std::unique_lock<std::mutex> lock_rm_q(this -> remove_mutex);
        if(this -> remove_queue.empty()) {
            return;
        }
        to_remove.swap(this -> remove_queue);
    }

    for(socket_t& sock_fd : to_remove) {
        {
            std::unique_lock<std::shared_mutex> lock_e_mod(this -> epoll_mod_mutex);
            this -> epoll_mod_map.erase(sock_fd);
        }
        epoll_ctl(this -> epoll_fd, EPOLL_CTL_DEL, sock_fd, nullptr);

        this -> read_buffers.erase(sock_fd);
        // clear read / write buffer
        {
            std::unique_lock<std::shared_mutex> lock_w_buf(this -> write_buffers_mutex);
            this -> write_buffers.erase(sock_fd);
        }
        
        {
        std::unique_lock<std::shared_mutex> lock_fd_m(this -> fd_type_map_mutex);
            std::unordered_map<socket_t, Fd_Type>::iterator found_fd = fd_type_map.find(sock_fd);
            if(found_fd != this -> fd_type_map.end()) {
                if(found_fd -> second == Fd_Type::PARTITION) {
                    std::unordered_map<socket_t, std::queue<Server_Message>>::iterator p_queue_it = this -> partition_queues.find(sock_fd);
                    std::queue<Server_Message> clients_to_err;
                    if(p_queue_it != this -> partition_queues.end()) {
                        clients_to_err = p_queue_it -> second;
                    }

                    while(!clients_to_err.empty()) {
                        Server_Message msg = clients_to_err.front();
                        clients_to_err.pop();
                        socket_t client_fd;
                        {
                            std::shared_lock<std::shared_mutex> lock(this -> id_client_map_mutex);
                            client_fd = this -> id_client_map[msg.get_cid()];
                        }

                        this -> queue_client_for_error_response(client_fd, msg.get_cid());
                    }

                    this -> partition_queues.erase(sock_fd);
                }
            }
            this -> fd_type_map.erase(sock_fd); 
        }

        close(sock_fd);
    }
}

socket_t Primary_Server::is_still_same_client(protocol_id_t client_id) {
    // get the socket from the clients id
    // think about race conditions in here....
    socket_t c_fd = 0;
    {
        std::shared_lock<std::shared_mutex> lock(this -> id_client_map_mutex);
        std::unordered_map<protocol_id_t, socket_t>::iterator id_c_it = this -> id_client_map.find(client_id);
        if(id_c_it != this -> id_client_map.end()) {
            c_fd = id_c_it -> second;
        }
        else {
            return -1;
        }
    }
    // get the id from the socket
    protocol_id_t other_cid;
    {
        std::shared_lock<std::shared_mutex> lock(this -> client_id_map_mutex);
        std::unordered_map<socket_t, protocol_id_t>::iterator c_id_t = this -> client_id_map.find(c_fd);
        if(c_id_t != client_id_map.end()) {
            other_cid = c_id_t -> second;
        }
        else {
            return -1;
        }
    }
    // if the match return true
    if(other_cid != client_id) {
        std::unique_lock<std::shared_mutex> lock(id_client_map_mutex);
        id_client_map.erase(client_id);
        return -1;
    }

    return c_fd;
}

void Primary_Server::remove_client(socket_t client_fd) {
    if(client_fd < 0) {
        return;
    }
    
    this -> read_buffers.erase(client_fd);

    {
        std::unique_lock<std::shared_mutex> lock(this -> write_buffers_mutex);
        this -> write_buffers.erase(client_fd);
    }

    {
        std::unique_lock<std::shared_mutex> lock(this -> client_cursor_map_mutex);
        this -> client_cursor_map.erase(client_fd);
    }

    protocol_id_t cid = 0;
    bool found = false;
    {
        std::unique_lock<std::shared_mutex> lock(this -> client_id_map_mutex);
        std::unordered_map<socket_t, protocol_id_t>::iterator c_id_it = this -> client_id_map.find(client_fd);
        if(c_id_it != this -> client_id_map.end()) {
            found = true;
            cid = c_id_it -> second;
            client_id_map.erase(client_fd);
        }
    }

    if(found) {
        std::unique_lock<std::shared_mutex> lock(this -> id_client_map_mutex);
        id_client_map.erase(cid);
    }

    {
        std::unique_lock<std::shared_mutex> lock(this -> fd_type_map_mutex);
        std::unordered_map<socket_t, Fd_Type>::iterator fd_t_it = this -> fd_type_map.find(client_fd);
        if(fd_t_it != this -> fd_type_map.end()) {
            if(fd_t_it -> second == Fd_Type::CLIENT) {
                fd_type_map.erase(client_fd);
            }
        }
    }
    
    {
        std::unique_lock<std::shared_mutex> lock_e_mod(this -> epoll_mod_mutex);
        this -> epoll_mod_map.erase(client_fd);
    }
    
    epoll_ctl(this -> epoll_fd, EPOLL_CTL_DEL, client_fd, nullptr);

    close(client_fd);
}

Cursor Primary_Server::extract_cursor_creation(const Server_Message& message) {
    cursor_name_len_t cursor_len = 0;
    protocol_msg_len_t pos = PROTOCOL_CURSOR_LEN_POS;
    
    if(pos + sizeof(cursor_name_len_t) > message.size()) {
        throw std::length_error(SERVER_MESSAGE_TOO_SHORT_ERR_MSG);
    }

    memcpy(&cursor_len, &message.c_str()[pos], sizeof(cursor_name_len_t));
    pos += sizeof(cursor_name_len_t);
    cursor_len = cursor_name_len_ntoh(cursor_len);

    if(pos + cursor_len > message.size()) {
        throw std::length_error(SERVER_MESSAGE_TOO_SHORT_ERR_MSG);
    }

    std::string cursor_name(cursor_len, '\0');
    memcpy(&cursor_name[0], &message.c_str()[pos], cursor_len);
    pos += cursor_len;

    protocol_key_len_t key_len = 0;
    if(pos + sizeof(protocol_key_len_t) > message.size()) {
        throw std::length_error(SERVER_MESSAGE_TOO_SHORT_ERR_MSG);
    }

    memcpy(&key_len, &message.c_str()[pos], sizeof(protocol_key_len_t));
    pos += sizeof(protocol_key_len_t);
    key_len = protocol_key_len_ntoh(key_len);

    if(pos + key_len > message.size()) {
        throw std::length_error(SERVER_MESSAGE_TOO_SHORT_ERR_MSG);
    }

    std::string key_str(key_len, '\0');
    memcpy(&key_str[0], &message.c_str()[pos], key_len);
    pos += key_len;


    Cursor cursor(cursor_name, message.get_cid());
    cursor.set_next_key(key_str, key_len);
    cursor.set_cid(message.get_cid());
    return cursor;
}

std::string Primary_Server::extract_cursor_name(const Server_Message& message) {
    cursor_name_len_t cursor_len = 0;
    protocol_msg_len_t pos = PROTOCOL_CURSOR_LEN_POS;
    
    if(pos + sizeof(cursor_name_len_t) > message.size()) {
        throw std::length_error(SERVER_MESSAGE_TOO_SHORT_ERR_MSG);
    }

    memcpy(&cursor_len, &message.c_str()[pos], sizeof(cursor_name_len_t));
    cursor_len = cursor_name_len_ntoh(cursor_len);
    pos += sizeof(cursor_name_len_t);

    if(pos + cursor_len > message.size()) {
        throw std::length_error(SERVER_MESSAGE_TOO_SHORT_ERR_MSG);
    }

    std::string cursor_name(cursor_len, '\0');
    memcpy(&cursor_name[0], &message.c_str()[pos], cursor_len);
    pos += cursor_len;

    return cursor_name;
}

std::pair<std::string, cursor_cap_t> Primary_Server::extract_cursor_name_cap(const Server_Message& message) {
    std::string name = this -> extract_cursor_name(message);

    cursor_cap_t cap = 0;

    memcpy(&cap, &message.c_str()[sizeof(protocol_msg_len_t) + sizeof(protocol_id_t) +  sizeof(protocol_array_len_t) - sizeof(cursor_cap_t)], sizeof(cursor_cap_t));
    cap = cursor_cap_ntoh(cap);

    if(cap > CURSOR_MAX_SIZE) {
        throw std::length_error(CURSOR_CAPACITY_TOO_BIG_ERR_MSG);
    }

    return std::make_pair(name, cap);
}

std::vector<Entry> Primary_Server::extract_got_entries_and_info(const Server_Message& message, Cursor_Info& curs_info, std::string& next_key_str) {
    protocol_array_len_t arr_len = this -> extract_array_size(message.string(), true);
    std::vector<Entry> entries;
    entries.reserve(arr_len);
    uint64_t pos = PROTOCOL_FIRST_KEY_LEN_POS;

    const char* data = message.c_str();

    for(protocol_array_len_t i = 0; i < arr_len; ++i) {
        protocol_key_len_t key_len = 0;
        memcpy(&key_len, &data[pos], sizeof(protocol_key_len_t));
        pos += sizeof(protocol_key_len_t);
        key_len = protocol_key_len_ntoh(key_len);

        std::string key_str(key_len, '\0');
        memcpy(&key_str[0], &data[pos], key_len);
        pos += key_len;

        protocol_value_len_t value_len = 0;
        memcpy(&value_len, &data[pos], sizeof(protocol_value_len_t));
        pos += sizeof(value_len);
        value_len = protocol_value_len_ntoh(value_len);

        std::string val_str(value_len, '\0');
        memcpy(&val_str[0], &data[pos], value_len);
        pos += value_len;

        entries.push_back(Entry(Bits(key_str), Bits(val_str)));

    }
    // pos is now at the next key
    protocol_key_len_t next_key_len = 0;
    memcpy(&next_key_len, &data[pos], sizeof(protocol_key_len_t));
    next_key_len = protocol_key_len_ntoh(next_key_len);
    pos += sizeof(protocol_key_len_t);

    std::string next_key(next_key_len, '\0');
    memcpy(&next_key[0], &data[pos], next_key_len);
    pos += next_key_len;

    // read the cursorlen and name
    cursor_name_len_t curs_name_len;
    memcpy(&curs_name_len, &data[pos], sizeof(cursor_name_len_t));
    pos += sizeof(cursor_name_len_t);
    curs_name_len = cursor_name_len_ntoh(curs_name_len);

    std::string cursor_name(curs_name_len, '\0');
    memcpy(&cursor_name[0], &data[pos], curs_name_len);

    // copy the returned values
    curs_info.name = std::move(cursor_name);
    curs_info.name_len = curs_name_len;
    next_key_str = std::move(next_key);

    return entries;
}

socket_t Primary_Server::find_client_fd(protocol_id_t client_id) {
    std::shared_lock<std::shared_mutex> lock(this -> id_client_map_mutex);
    std::unordered_map<protocol_id_t, socket_t>::iterator it = this -> id_client_map.find(client_id);
    if(it != this -> id_client_map.end()) {
        return it -> second;
    }

    return -1;
}


Cursor Primary_Server::locate_cursor(socket_t client_fd, std::string& cursor_name) {
    std::shared_lock<std::shared_mutex> lock(this -> client_cursor_map_mutex);
    std::unordered_map<socket_t, std::unordered_map<std::string, Cursor>>::iterator client_it = this -> client_cursor_map.find(client_fd);
    if(client_it == this -> client_cursor_map.end()) {
        throw std::runtime_error(PRIMARY_SERVER_CURSOR_FIND_FAILED_ERR_MSG);
    }

    std::unordered_map<std::string, Cursor>::iterator cursor_it = client_it -> second.find(cursor_name);
    if(cursor_it == client_it -> second.end()) {
        throw std::runtime_error(PRIMARY_SERVER_CURSOR_FIND_FAILED_ERR_MSG);
    }

    return cursor_it -> second;
}

void Primary_Server::return_cursor(socket_t client_fd, Cursor&& cursor) {
    std::shared_lock<std::shared_mutex> lock(this -> client_cursor_map_mutex);
    std::unordered_map<socket_t, std::unordered_map<std::string, Cursor>>::iterator client_it = this -> client_cursor_map.find(client_fd);
    if(client_it == this -> client_cursor_map.end()) {
        throw std::runtime_error(PRIMARY_SERVER_CURSOR_FIND_FAILED_ERR_MSG);
    }

    std::unordered_map<std::string, Cursor>::iterator cursor_it = client_it -> second.find(cursor.get_name());
    if(cursor_it == client_it -> second.end()) {
        throw std::runtime_error(PRIMARY_SERVER_CURSOR_FIND_FAILED_ERR_MSG);
    }

    cursor_it -> second = std::move(cursor);
}

int8_t Primary_Server::query_partition_by_cursor(Cursor& cursor, Command_Code com_code, protocol_id_t client_id, bool edge_fb_case) {    
    // construct be response
    protocol_msg_len_t msg_len = sizeof(protocol_msg_len_t) + sizeof(command_code_t) + sizeof(protocol_id_t) + sizeof(protocol_array_len_t) + sizeof(protocol_key_len_t) + cursor.get_next_key_size() + sizeof(cursor_cap_t) + cursor.get_name_size();
    std::string msg_str(msg_len, '\0');

    uint64_t pos = 0;

    protocol_msg_len_t net_msg_len = protocol_msg_len_hton(msg_len);
    memcpy(&msg_str[0], &net_msg_len, sizeof(protocol_msg_len_t));
    pos += sizeof(protocol_msg_len_t);

    protocol_id_t net_cid = protocol_id_hton(client_id);
    memcpy(&msg_str[pos], &net_cid, sizeof(protocol_id_t));
    pos += sizeof(protocol_id_t);

    protocol_array_len_t net_arr_len = protocol_arr_len_hton(1);
    memcpy(&msg_str[pos], &net_arr_len, sizeof(protocol_array_len_t));
    pos += sizeof(protocol_array_len_t);

    command_code_t net_com_code = command_hton(com_code);
    memcpy(&msg_str[pos], &net_com_code, sizeof(command_code_t));
    pos += sizeof(command_code_t);

    protocol_key_len_t net_key_len = protocol_key_len_hton(cursor.get_next_key_size());
    memcpy(&msg_str[pos], &net_key_len, sizeof(protocol_key_len_t));
    pos += sizeof(protocol_key_len_t);

    memcpy(&msg_str[pos], &cursor.get_next_key()[0], cursor.get_next_key_size());
    pos += cursor.get_next_key_size();

    cursor_name_len_t net_name_len = cursor_name_len_hton(cursor.get_name_size());
    memcpy(&msg_str[pos], &net_name_len, sizeof(cursor_name_len_t));
    pos += sizeof(cursor_name_len_t);

    memcpy(&msg_str[pos], &cursor.get_name()[0], cursor.get_name_size());
    
    pos = PROTOCOL_EDGE_FB_FLAG_POS;
    if(edge_fb_case) {
        uint8_t flag = 1;
        memcpy(&msg_str[pos], &flag, sizeof(uint8_t));
    }
    pos += 1;
    cursor_cap_t net_cap = cursor_cap_hton(cursor.get_remaining());
    memcpy(&msg_str[pos], &net_cap, sizeof(cursor_cap_t));

    Server_Message serv_msg;
    serv_msg.set_cid(client_id);
    serv_msg.set_message_eat(std::move(msg_str));

    socket_t partition_sock = 0;
    {
        std::shared_lock<std::shared_mutex> lock(this -> partitions_mutex);
        partition_sock = this -> partitions[cursor.get_last_called_part_id()].socket_fd;
    }
    this -> queue_partition_for_response(partition_sock, std::move(serv_msg));
    return 0;
}