#include "../include/partition_server.h"
#include "../include/primary_server.h"
#include <cstdlib>

#define BAD_ARGUMENT_COUNT_ERR_MSG ""
#define REQUIRED_ARGUMENT_COUNT "2"
#define ARGUMENT_FORMAT "./server <SERVER_TYPE> <PORT>\n"

#define ALLOWED_PORT_RANGE_MSG "Allowed port range is ..."
#define BAD_PORT_NUMBER_MSG "Bad port number\n"
#define MIN_PORT_NUMBER 49152

#define BAD_SERVER_TYPE_MSG "Provided server type is incorrect\n"
#define HELP_MSG "Try \"./server ?\" for more information\n" 
#define HELP_SYMBOL '?'

typedef enum Server_Type {
    PRIMARY_SERVER,
    PARTITION_SERVER
} Server_Type;

// arguments must be passed like [server_type] [port]
int main(int argc, char* argv[]) {
    if(argc != 3) {
        // check for ? argument
        if(argc == 2 && argv[1][0] == HELP_SYMBOL) {
            // print help msg
            return 0;
        }
        return -1;
    }

    // get which server to create
    uint8_t server_type = atoi(argv[1]);

    // check the port if its valid
    uint16_t port = atoi(argv[2]);

    /*if(port < MIN_PORT_NUMBER){
        std::cerr << BAD_PORT_NUMBER_MSG << ALLOWED_PORT_RANGE_MSG;
        return -1;
    }*/

    try {
        switch(server_type) {
            case PRIMARY_SERVER: {
                Primary_Server primary_server(port);
                return primary_server.start();
                break;
            }
            case PARTITION_SERVER: {
                Partition_Server partition_server(port);
                return partition_server.start();
                break;
            }
            default: {
                std::cerr << BAD_SERVER_TYPE_MSG;
                return -1;
                break;
            }
        }
    }
    catch(const std::exception& e) {
        std::cerr << e.what() << std::endl;
    }

    return 0;
}
