#include <iostream>
#include <string>
#include <cstdlib>
#include <cstdio>
#include <csignal>
#include <cstring>

#ifndef _MSC_VER
#include <sys/time.h>
#endif

#ifdef _MSC_VER
#include "../win32/wingetopt.h"
#include <atltime.h>
#elif _AIX
#include <unistd.h>
#else
#include <getopt.h>
#include <unistd.h>
#endif

/*
* Typically include path in a real application would be
* #include <librdkafka/rdkafkacpp.h>
*/
#include <librdkafka/rdkafkacpp.h>

typedef void (* msg_callback)(std::string);

static bool run = true;
static bool exit_eof = false;
static int eof_cnt = 0;
static int partition_cnt = 0;
static int verbosity = 1;
static long msg_cnt = 0;
static int64_t msg_bytes = 0;
static void sigterm (int sig) {
    run = false;
}


/**
 * @brief format a string timestamp from the current time
 */
static void print_time () {
#ifndef _MSC_VER
    struct timeval tv;
    char buf[64];
    gettimeofday(&tv, NULL);
    strftime(buf, sizeof(buf) - 1, "%Y-%m-%d %H:%M:%S", localtime(&tv.tv_sec));
    fprintf(stderr, "%s.%03d: ", buf, (int)(tv.tv_usec / 1000));
#else
    std::wcerr << CTime::GetCurrentTime().Format(_T("%Y-%m-%d %H:%M:%S")).GetString()
               << ": ";
#endif
}
class ExampleEventCb : public RdKafka::EventCb {
public:
    void event_cb (RdKafka::Event &event) {

        print_time();

        switch (event.type()) {
        case RdKafka::Event::EVENT_ERROR:
            if (event.fatal()) {
                std::cerr << "FATAL ";
                run = false;
            }
            std::cerr << "ERROR (" << RdKafka::err2str(event.err()) << "): " <<
                      event.str() << std::endl;
            break;

        case RdKafka::Event::EVENT_STATS:
            std::cerr << "\"STATS\": " << event.str() << std::endl;
            break;

        case RdKafka::Event::EVENT_LOG:
            fprintf(stderr, "LOG-%i-%s: %s\n",
                    event.severity(), event.fac().c_str(), event.str().c_str());
            break;

        case RdKafka::Event::EVENT_THROTTLE:
            std::cerr << "THROTTLED: " << event.throttle_time() << "ms by " <<
                      event.broker_name() << " id " << (int)event.broker_id() << std::endl;
            break;

        default:
            std::cerr << "EVENT " << event.type() <<
                      " (" << RdKafka::err2str(event.err()) << "): " <<
                      event.str() << std::endl;
            break;
        }
    }
};


class ExampleRebalanceCb : public RdKafka::RebalanceCb {
private:
    static void part_list_print (const std::vector<RdKafka::TopicPartition *> &partitions) {
        for (unsigned int i = 0 ; i < partitions.size() ; i++)
            std::cerr << partitions[i]->topic() <<
                      "[" << partitions[i]->partition() << "], ";
        std::cerr << "\n";
    }

public:
    void rebalance_cb (RdKafka::KafkaConsumer *consumer,
                       RdKafka::ErrorCode err,
                       std::vector<RdKafka::TopicPartition *> &partitions) {
        std::cerr << "RebalanceCb: " << RdKafka::err2str(err) << ": ";

        part_list_print(partitions);

        if (err == RdKafka::ERR__ASSIGN_PARTITIONS) {
            consumer->assign(partitions);
            partition_cnt = (int)partitions.size();
        } else {
            consumer->unassign();
            partition_cnt = 0;
        }
        eof_cnt = 0;
    }
};


void msg_consume(RdKafka::Message *message, void *opaque, msg_callback callback) {
    switch (message->err()) {
    case RdKafka::ERR__TIMED_OUT:
        break;

    case RdKafka::ERR_NO_ERROR:
        /* Real message */
        msg_cnt++;
        msg_bytes += message->len();
        if (verbosity >= 3)
            std::cerr << "Read msg at offset " << message->offset() << std::endl;
        RdKafka::MessageTimestamp ts;
        ts = message->timestamp();
        if (verbosity >= 2 &&
                ts.type != RdKafka::MessageTimestamp::MSG_TIMESTAMP_NOT_AVAILABLE) {
            std::string tsname = "?";
            if (ts.type == RdKafka::MessageTimestamp::MSG_TIMESTAMP_CREATE_TIME)
                tsname = "create time";
            else if (ts.type == RdKafka::MessageTimestamp::MSG_TIMESTAMP_LOG_APPEND_TIME)
                tsname = "log append time";
            std::cout << "Timestamp: " << tsname << " " << ts.timestamp << std::endl;
        }
        if (verbosity >= 2 && message->key()) {
            std::cout << "Key: " << *message->key() << std::endl;
        }
        if (verbosity >= 1) {
            printf("%.*s\n",
                   static_cast<int>(message->len()),
                   static_cast<const char *>(message->payload()));
            std::string msg_cb_param(static_cast<const char *>(message->payload()));
            callback(msg_cb_param);

        }
        break;

    case RdKafka::ERR__PARTITION_EOF:
        /* Last message */
        if (exit_eof && ++eof_cnt == partition_cnt) {
            std::cerr << "%% EOF reached for all " << partition_cnt <<
                      " partition(s)" << std::endl;
            run = false;
        }
        break;

    case RdKafka::ERR__UNKNOWN_TOPIC:
    case RdKafka::ERR__UNKNOWN_PARTITION:
        std::cerr << "Consume failed: " << message->errstr() << std::endl;
        run = false;
        break;

    default:
        /* Errors */
        std::cerr << "Consume failed: " << message->errstr() << std::endl;
        run = false;
    }
}

/*
function consume takes a function as an input
which is performed whenever a message is consumed.
corresponding to a topic



use :

void msg_callback(std::string msg) {
	std::cout << msg << std::endl;
}

string topic = "packets";
string group_id = "consumer_group_1";

consume(topic, group_id, msg_callback);

*/

void consume (std::string topic_to_sub, std::string group_id, msg_callback callback) {
    // using base config
    std::string brokers = "localhost";
    std::string errstr;
    std::string topic_str;
    std::string mode;
    std::string debug;
    std::vector<std::string> topics;
    bool do_conf_dump = false;
    int opt;

    /*
    * Create configuration objects
    */
    RdKafka::Conf *conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
    RdKafka::Conf *tconf = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);

    ExampleRebalanceCb ex_rebalance_cb;
    conf->set("rebalance_cb", &ex_rebalance_cb, errstr);

    conf->set("enable.partition.eof", "true", errstr);

    conf->set("group.id",  group_id, errstr);


    topics.push_back(topic_to_sub);

    /*
    * Set configuration properties
    */
    conf->set("metadata.broker.list", brokers, errstr);

    if (!debug.empty()) {
        if (conf->set("debug", debug, errstr) != RdKafka::Conf::CONF_OK) {
            std::cerr << errstr << std::endl;
            exit(1);
        }
    }

    ExampleEventCb ex_event_cb;
    conf->set("event_cb", &ex_event_cb, errstr);

    if (do_conf_dump) {
        int pass;

        for (pass = 0 ; pass < 2 ; pass++) {
            std::list<std::string> *dump;
            if (pass == 0) {
                dump = conf->dump();
                std::cout << "# Global config" << std::endl;
            } else {
                dump = tconf->dump();
                std::cout << "# Topic config" << std::endl;
            }

            for (std::list<std::string>::iterator it = dump->begin();
                    it != dump->end(); ) {
                std::cout << *it << " = ";
                it++;
                std::cout << *it << std::endl;
                it++;
            }
            std::cout << std::endl;
        }
        exit(0);
    }

    conf->set("default_topic_conf", tconf, errstr);
    delete tconf;

    signal(SIGINT, sigterm);
    signal(SIGTERM, sigterm);


    /*
    * Consumer mode
    */

    /*
    * Create consumer using accumulated global configuration.
    */
    RdKafka::KafkaConsumer *consumer = RdKafka::KafkaConsumer::create(conf, errstr);
    if (!consumer) {
        std::cerr << "Failed to create consumer: " << errstr << std::endl;
        exit(1);
    }

    delete conf;

    std::cout << "% Created consumer " << consumer->name() << std::endl;


    /*
    * Subscribe to topics
    */
    RdKafka::ErrorCode err = consumer->subscribe(topics);
    if (err) {
        std::cerr << "Failed to subscribe to " << topics.size() << " topics: "
                  << RdKafka::err2str(err) << std::endl;
        exit(1);
    }

    /*
    * Consume messages
    */
    while (run) {
        RdKafka::Message *msg = consumer->consume(1000);
        msg_consume(msg, NULL, callback);
        delete msg;
    }

#ifndef _MSC_VER
    alarm(10);
#endif

    consumer->close();
    delete consumer;

    std::cerr << "% Consumed " << msg_cnt << " messages ("
              << msg_bytes << " bytes)" << std::endl;

    RdKafka::wait_destroyed(5000);
}
