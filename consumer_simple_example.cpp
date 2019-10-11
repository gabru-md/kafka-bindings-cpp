#include <string>
#include "include/raw_consumer.h"

void callback(std::string msg) {
    std::cout << msg << std::endl;
}

int main(void) {
    std::string topic = "test-topic-1";
    std::string group_id = "consumer_group_1";
    consume(topic, group_id, callback);
    return 0;
}