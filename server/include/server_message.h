#ifndef YSQL_SERVER_MESSAGE_H_INCLUDED
#define YSQL_SERVER_MESSAGE_H_INCLUDED

#include "protocol.h"
#include <iostream>

class Server_Message {
    private:
        std::string msg;
        protocol_msg_len_t bytes_processed;
        protocol_msg_len_t bytes_to_process;
        protocol_id_t cid;

    public:
        Server_Message(std::string& message, protocol_id_t client_id);

        Server_Message();

        Server_Message(std::string& message);

        // returns the internal message as a string
        std::string string() const;
        
        // removes client_id from the message string, but keeps it internally
        // modifies bytes_to_process
        void remove_cid();

        // adds a client id to internal structure
        void add_cid(protocol_id_t client_id);

        // returns the client id that the message belongs to
        protocol_id_t get_cid() const;
        
        // returns the bytes left to process;
        protocol_msg_len_t to_process() const;

        // set bytes to process
        void set_to_process(protocol_msg_len_t bytes_to_process);

        // add bytes_processed to current amount of processed bytes
        void add_processed(protocol_msg_len_t bytes_processed);

        void reset_processed();

        // returns the amount of bytes processed
        protocol_msg_len_t processed() const;

        // return true if message is empty
        bool is_empty() const;

        bool is_fully_read() const;

        void append_string(std::string string_to_append, size_t bytes_to_append);

        // literally eats the string, dont expect it back
        void set_message_eat(std::string &&new_msg);

        void set_cid(protocol_id_t client_id);

        void reset();

        std::string& get_string_data();

        // for testing
        void print() const;

        // clears everything except CID
        void clear();

        protocol_msg_len_t size() const;

        const char* c_str() const;
};

#endif // YSQL_SERVER_MESSAGE_H_INCLUDED
