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



TRabbitMQ::TRabbitMQ(string host, string queue)
  : host_(host),
    queue(queue),
	connectionOpen(false),
	amqp(host_){
		ex = amqp.createExchange("e");
		ex->Declare("e", "fanout");

		qu2 = amqp.createQueue(queue);
		qu2->Declare();
		qu2->Bind( "e", "");

	}

TRabbitMQ::TRabbitMQ(string queue)
  : host_("localhost"),
    queue(queue){
}

TRabbitMQ::~TRabbitMQ() {
  close();
}

bool TRabbitMQ::isOpen() {
  return (connectionOpen);
}

bool TRabbitMQ::peek() {
	cout << "TRabbitMQ::peek()" <<endl;
	return inputBuffer.size() > 0;
}


void TRabbitMQ::open() {
  cout << "TRabbitMQ::open()" << endl;
  if (isOpen()) {
    return;
  }
  try{
  	//AMQP amqp(host_);
/*
	AMQPExchange * ex = amqp.createExchange("e");
	ex->Declare("e", "fanout");

	AMQPQueue * qu2 = amqp.createQueue(queue);
		qu2->Declare();
		qu2->Bind( "e", "");

		string ss = "message 1 ";

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
		connectionOpen = true;
	} catch (AMQPException e) {
		std::cout << e.getMessage() << std::endl;
	}
  cout << "TRabbitMQ::open() -- FINISHED" << endl;

}


	void TRabbitMQ::close() {
		cout << "TRabbitMQ::close()" <<endl;
	}

	int onCancel(AMQPMessage * message ) {
		cout << "cancel tag="<< message->getDeliveryTag() << endl;
		return 0;
	}

	int  onMessage( AMQPMessage * message  ) {
		cout << "onMessage()" <<endl;
		uint32_t j = 0;
		char * data = message->getMessage(&j);
		if (data)
			cout << data << endl;


		cout  << " tag="<< message->getDeliveryTag() << " content-type:"<< message->getHeader("Content-type") ;
		cout << " encoding:"<< message->getHeader("Content-encoding")<< " mode="<<message->getHeader("Delivery-mode")<<endl;

		return 0;
	};



void TRabbitMQ::write(const uint8_t* buf, uint32_t len) {
	cout << "TRabbitMQ::write()" << endl;
	boost::uuids::uuid uuid = boost::uuids::random_generator()();
	response = "";
	corr_id = boost::lexical_cast<std::string>(uuid);

	AMQPQueue * qu2 = amqp.createQueue(queue);
	qu2->Declare();
	qu2->Bind( "e", "");

	std::string msg = std::string((const char*)buf, (size_t)len);
	cout << "Setting correlation_id: " <<corr_id<<endl;
	ex->setHeader("correlation_id", corr_id);
	ex->setHeader("Delivery-mode", 2);
	ex->setHeader("Content-type", "text/text");

	AMQPQueue * qu3 = amqp.createQueue(corr_id);
	qu3->Declare();
	qu3->Bind( "e", "");
	qu3->addEvent(AMQP_MESSAGE, onMessage );
	qu3->addEvent(AMQP_CANCEL, onCancel );
	ex->setHeader("Reply-to", qu3->getName());
	ex->Publish(msg, "");
	cout << "TRabbitMQ::write() published: " << msg << endl;


	qu3->Consume(AMQP_NOACK);//
}

uint32_t TRabbitMQ::write_partial(const uint8_t* buf, uint32_t len) {
//TODO:
	cout << "TRabbitMQ::write_partial()" << endl;
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
	cout << "TRabbitMQ::read()" << endl;
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
