CXX = g++
LFLAGS = -lrdkafka++

consumer:
	$(CXX) -I"/usr/local/include/librdkafka" consumer_simple_example.cpp -o consumer -lrdkafka++

producer:
	$(CXX) -I"/usr/local/include/librdkafka" producer_simple_example.cpp -o producer -lrdkafka++



# g++ -g -O2 -fPIC -Wall -Wsign-compare -Wfloat-equal -Wpointer-arith -Wcast-align -Wno-non-virtual-dtor -I../src-cpp rdkafka_consumer_example.cpp -o rdkafka_consumer_example_cpp  \
	../src-cpp/librdkafka++.a ../src/librdkafka.a -lm -lssl -lcrypto -lz -ldl -lpthread -lrt -lstdc++
