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

#include <thrift/concurrency/Monitor.h>
#include <thrift/transport/TRabbitMQ.h>
#include <thrift/transport/TTransportException.h>
#include <thrift/transport/PlatformSocket.h>
#include "AMQPcpp.h"

namespace apache {
namespace thrift {
namespace transport {

using namespace std;


/**
 * TRabbitMQ implementation.
 *
 */

TRabbitMQ::TRabbitMQ(string host, string queue)
  : host_(host),
    queue(queue){
}

TRabbitMQ::TRabbitMQ(string queue)
  : host_("localhost"),
    queue(queue){
}

TRabbitMQ::~TRabbitMQ() {
  close();
}

bool TRabbitMQ::isOpen() {
  return (true);
}

bool TRabbitMQ::peek() {
	return inputBuffer.size() > 0;
}


void TRabbitMQ::open() {
  if (isOpen()) {
    return;
  }
  try{
  	AMQP amqp(host_);

	AMQPExchange * ex = amqp.createExchange("e");
	ex->Declare("e", "fanout");

	AMQPQueue * qu2 = amqp.createQueue("q2");
		qu2->Declare();
		qu2->Bind( "e", "");		

		string ss = "message 1 ";
		/* test very long message
		ss = ss+ss+ss+ss+ss+ss+ss;
		ss += ss+ss+ss+ss+ss+ss+ss;
		ss += ss+ss+ss+ss+ss+ss+ss;
		ss += ss+ss+ss+ss+ss+ss+ss;
		ss += ss+ss+ss+ss+ss+ss+ss;
*/

		ex->setHeader("Delivery-mode", 2);
		ex->setHeader("Content-type", "text/text");
		//ex->setHeader("Content-encoding", "UTF-8");

		ex->Publish(  ss , ""); // publish very long message
		
		ex->Publish(  "message 2 " , "");
		ex->Publish(  "message 3 " , "");
		
		/*
		if (argc==2) {
			AMQPQueue * qu = amqp.createQueue();			
			qu->Cancel(   amqp_cstring_bytes(argv[1]) );
		}*/												
						
	} catch (AMQPException e) {
		std::cout << e.getMessage() << std::endl;
	}

}


void TRabbitMQ::close() {
}



void TRabbitMQ::write(const uint8_t* buf, uint32_t len) {
	//TODO:
}

uint32_t TRabbitMQ::write_partial(const uint8_t* buf, uint32_t len) {
//TODO:
}


void TRabbitMQ::setLinger(bool on, int linger) {
	return;
}

void TRabbitMQ::setNoDelay(bool noDelay) {
	return;
}

void TRabbitMQ::setConnTimeout(int ms) {
  return;
}

uint32_t TRabbitMQ::read(uint8_t* buf, uint32_t len) {
	//TODO:
	return len;
}

string TRabbitMQ::geTRabbitMQInfo() {
  std::ostringstream oss;
  oss << "<Host: " << host_ << " Queue: " << queue << ">";
  
  return oss.str();
}

}
}
} // apache::thrift::transport
