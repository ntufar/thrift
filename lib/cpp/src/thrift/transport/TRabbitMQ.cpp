/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include <thrift/thrift-config.h>
#include <sstream>
#include <thread>

#include <thrift/concurrency/Monitor.h>
#include <thrift/transport/TRabbitMQ.h>
#include <thrift/transport/TTransportException.h>
#include <thrift/transport/PlatformSocket.h>


#include <boost/uuid/uuid.hpp>            // uuid class
#include <boost/uuid/uuid_generators.hpp> // generators
#include <boost/uuid/uuid_io.hpp>         // streaming operators etc.
#include <boost/lexical_cast.hpp>

namespace apache {
    namespace thrift {
        namespace transport {

            using namespace std;

/**
* TRabbitMQ implementation.
*
*/


            TRabbitMQ *this_instance;

            TRabbitMQ::TRabbitMQ(string host, string queue)
                    :
                    responce_available(false),
                    host_(host),
                    queue(queue),
                    connectionOpen(false),
                    amqp(host_) {
                this_instance = this;
                ex = amqp.createExchange("e");
                ex->Declare("e", "fanout");

                exr = amqp.createExchange("e2");
                exr->Declare("e2", "fanout");

            }

            TRabbitMQ::TRabbitMQ(string queue)
                    : host_("localhost"),
                      queue(queue) {
            }

            TRabbitMQ::~TRabbitMQ() {
                close();
            }

            bool TRabbitMQ::isOpen() {
                return (connectionOpen);
            }

            bool TRabbitMQ::peek() {
                cout << "TRabbitMQ::peek()" << endl;
                return inputBuffer.size() > 0;
            }


            void TRabbitMQ::open() {
                cout << "TRabbitMQ::open()" << endl;
                if (isOpen()) {
                    return;
                }
                try {
                    connectionOpen = true;
                } catch (AMQPException e) {
                    std::cout << e.getMessage() << std::endl;
                }
                cout << "TRabbitMQ::open() -- FINISHED" << endl;

            }


            void TRabbitMQ::close() {
                connectionOpen = false;
                cout << "TRabbitMQ::close()" << endl;
            }

            int onCancel(AMQPMessage *message) {
                cout << "cancel tag=" << message->getDeliveryTag() << endl;
                return 0;
            }

            int onMessage(AMQPMessage *message) {
                cout << "onMessage() queue: " << message->getQueue()->getName() << endl;
                uint32_t j = 0;
                char *data = message->getMessage(&j);
                this_instance->responce_data = (uint8_t *) data;
                this_instance->responce_available = true;
                if (data)
                    cout << data << endl;


                cout << " tag=" << message->getDeliveryTag() << " content-type:" << message->getHeader("Content-type");
                cout << " encoding:" << message->getHeader("Content-encoding") << " mode=" << message->getHeader("Delivery-mode") << endl;

                AMQPQueue *q = message->getQueue();
                q->Cancel(message->getConsumerTag());

                return 0;
            }

            void TRabbitMQ::consume_messages_thread() {
                cout << "Starting listening thread" << endl;
                responseQueue->Consume(AMQP_NOACK);//
            }

            void TRabbitMQ::write(const uint8_t *buf, uint32_t len) {
                cout << "TRabbitMQ::write()" << endl;
                boost::uuids::uuid uuid = boost::uuids::random_generator()();
                response = "";
                corr_id = boost::lexical_cast<std::string>(uuid);

                AMQPQueue *qu2 = amqp.createQueue(queue);
                qu2->Declare();
                qu2->Bind("e", "");

                std::string msg = std::string((const char *) buf, (size_t) len);
                cout << "Setting correlation_id: " << corr_id << endl;
                ex->setHeader("correlation_id", corr_id);
                ex->setHeader("Delivery-mode", 2);
                ex->setHeader("Content-type", "text/text");

                responseQueue = amqp.createQueue(corr_id);
                responseQueue->Declare();
                responseQueue->Bind("e2", "");
                responseQueue->addEvent(AMQP_MESSAGE, onMessage);
                responseQueue->addEvent(AMQP_CANCEL, onCancel);

                ex->setHeader("Reply-to", responseQueue->getName());
                ex->Publish(msg, "");
                cout << "TRabbitMQ::write() published: " << msg << endl;

                qu2->closeChannel();
                std::thread(&TRabbitMQ::consume_messages_thread, this).detach();
            }

            uint32_t TRabbitMQ::read(uint8_t *buf, uint32_t len) {
                while (responce_available != true)
                    usleep(100);
                size_t rdlen = strlen((const char *) responce_data);
                size_t l = rdlen < len ? rdlen : len;
                memcpy(buf, responce_data, l);
                responce_available = false;
                cout << "TRabbitMQ::read(len=" << l << ")" << endl;
                return l;
            }


        }
    }
} // apache::thrift::transport
