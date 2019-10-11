#include <string>
#include "include/raw_producer.h"

int main(void) {
    int i = 0;
    std::string topic = "test-topic-1";
    std::string brokers = "localhost:9092";
    std::string messasge = "this is a test message";
    while(i++<10) {
        produce(brokers, topic, messasge);
    }
    return 0;
}