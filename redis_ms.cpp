Redis client
https://github.com/brianwatling/redispp
https://github.com/brianwatling/redispp
https://github.com/Levhav/SimpleRedisClient/tree/master/for-redis-2.8

######## send message ########
#include <stdio.h>
#include <string>
#include "mq.h"

using namespace mq;
using namespace std;

const char* TEST_PORT = "6379";
const char* TEST_HOST = "127.0.0.1";
int main(int argc, char* argv[])
{
    if(argc < 2) {
        printf("\nUsage: test_me message\n");
        return 0;
    }

    Connection conn(TEST_HOST, TEST_PORT, "");
    string key = "queue:updater";
    string message ="";
    char buffer[1024];

    // atoi(argv[1]);
    sprintf(buffer,"{\"class\":\"Updater\",\"args\":\"%s\"}",argv[1]);
    message=buffer;
    IntReply reply=conn.rpush(key, message);

    int length = conn.llen(key);
    printf("\nThe message queue length: %d.\nIt conatins:\n", length);
    MultiBulkEnumerator result = conn.lrange(key, 0, length);

    string data;
    while (result.next(&data)) {
        printf("%s\n", data.c_str());
    }
    printf("\n");
    return 0;
}

######## create message ########
#include <vector>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <boost/foreach.hpp>

using namespace std;
using boost::property_tree::ptree;
int main () {
    try{
        string response = "{\"id\":123,\"table\":\"hsm\"}";
    // read string into json
        ptree pt;
        stringstream ss; ss << response;
        read_json(ss, pt);

        ptree msg;
        msg.put("class", "Updated");
        msg.put("args", response);
    // output
        std::stringstream so;
        write_json(so, msg);
        string out = so.str();
        cout << out;

    }
    catch (exception& e)
    {
        cout << "Exception: " << e.what();
    }
}
