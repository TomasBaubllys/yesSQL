#include "../include/partition_server.h"
#include "../include/primary_server.h"
#include <cstdlib>
#include <system_error>

#define BAD_ARGUMENT_COUNT_ERR_MSG ""
#define REQUIRED_ARGUMENT_COUNT "2"
#define ARGUMENT_FORMAT "./server <SERVER_TYPE> <PORT>\n"

#define ALLOWED_PORT_RANGE_MSG "Allowed port range is ..."
#define BAD_PORT_NUMBER_MSG "Bad port number\n"

#define HELP_MSG "Missing or incorrect arguments: <server_type>:\n 0 - Primary, 1 - Partition\n<port> - must be a valid port\n<verbose> 0 - no error logging\n1 - log errors to stderr\n"

typedef enum Server_Type {
    PRIMARY_SERVER,
    PARTITION_SERVER
} Server_Type;

// arguments must be passed like <server_type> <port> <verbose>
int main(int argc, char* argv[]) {
    if(argc != 4) {
		std::cout << HELP_MSG << std::endl;
		return -1;
    }

    // get which server to create
    uint8_t server_type = atoi(argv[1]);

    // check the port if its valid
    uint16_t port = atoi(argv[2]);

    uint8_t verbose = atoi(argv[3]);

    /*if(port < MIN_PORT_NUMBER){
        std::cerr << BAD_PORT_NUMBER_MSG << ALLOWED_PORT_RANGE_MSG;
        return -1;
    }*/
    uint32_t thread_pool_size = SERVER_DEFAULT_THREAD_POOL_VAL;


    try {
        switch(server_type) {
            case PRIMARY_SERVER: {
                const char* thread_pool_size_str = std::getenv(PRIMARY_SERVER_THREAD_POOL_SIZE_ENV_VAR);
                if(thread_pool_size_str) {
                    thread_pool_size = atoi(thread_pool_size_str);
                }

                Primary_Server primary_server(port, verbose, thread_pool_size);
                return primary_server.start();
                break;
            }
            case PARTITION_SERVER: {
                const char* thread_pool_size_str = std::getenv(PARTITION_SERVER_THREAD_POOL_SIZE_ENV_VAR);
                if(thread_pool_size_str) {
                    thread_pool_size = atoi(thread_pool_size_str);
                }
                Partition_Server partition_server(port, verbose, thread_pool_size);
                return partition_server.start();
                break;
            }
            default: {
                std::cerr << HELP_MSG;
                return -1;
                break;
            }
        }
    }
    catch(const std::system_error& e) {
        std::cerr << e.what() << std::endl;
        std::cerr << e.code() << std::endl;
    }
    catch(const std::exception& e) {
        std::cerr << e.what() << std::endl;
    }

    return 0;
}
