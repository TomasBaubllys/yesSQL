#include "../include/server.h"

/* first create a all child servers (partitions)
 * iniatialize main server
 * wait for user requests
 * redirect users request base on the index range
 * return request from the main server 
 * USER -> MAIN_SERVER -> partition1
 *                      -> partition2
 *                      -> ..........
 *                      -> partitionN
 */

int main(int argc, char* argv[]) {
    Server server(8080);
    server.start();
    return 0;
}
