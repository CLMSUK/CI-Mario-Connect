/*
Copyright (C) 2016 CLMS UK LIMITED

This file is part of CLMS Connectivity Infrastructure
Connection Manager package.

Authors:
Antonis Migiakis <a.migiakis@clmsuk.com>
Maria Karanasou <m.karanasou@clmsuk.com>

Description:
Connection manager for CLMS Connectivity Infrastructure (Mario Connect)
*/
'use strict';

var SMP = require('service-metadata-provider');
var Promise = require('bluebird');
var nconf = require('nconf');
var sen = require('seneca');
var uuid = require('node-uuid');

nconf.argv()
.env()
.file({ file: 'config.json' });

var amqp = require('amqplib').connect('amqp://' + nconf.get("username") + ':' + nconf.get("password") + '@' + nconf.get("mariohost"));

/**
*  @constructor
*  Constructor for MarioConnect
*  @senecaNode {string} The seneca object
*  @concept {string} The name of the concept whose services I wish to serve
*  @action {string} The name of the specific action I wish to serve
*/
var MarioConnect = function (senecaNode, concept, action, participantIdentifier) {
  this.seneca = senecaNode;
  if(!this.seneca) {
    this.seneca = sen();
  }
  this.participantIdentifier = participantIdentifier || nconf.get("participantIdentifier");
  this.concept = concept;
  this.action = action;
  this.listeners = [];
  this.publishers = [];
};

MarioConnect.prototype.envelopeMessage = function(message, concept, action, initialRequest, error, source) {
  var envelope = {};
  if(initialRequest) {
    envelope.status = error ? "Error" : "Success";
    envelope.correlationid = initialRequest.messageid;
  }
  envelope.messageid = uuid.v1();
  envelope.source = source ? source : nconf.get("participantIdentifier"); //TODO - evaluate
  envelope.concept = concept;
  envelope.action = action;
  envelope.timestamp = new Date();
  envelope.payloadMimeType = "application/json"; // TODO - ????? json
  envelope.payloadType = ""; // TODO - "????? ParticipantType"
  envelope.content = message;
  return envelope;
};

MarioConnect.prototype.status = function() {
  var self = this;
  console.log("Listeners:");
  self.listeners.forEach(function (element, index, array) {
    console.log("\t" + element);
  });
  console.log("Publishers:");
  self.publishers.forEach(function (element, index, array) {
    console.log("\t" + element);
  });
};

MarioConnect.prototype.start = function() {
  var self = this;
  var marioConnection;
  var open = amqp.then(function(conn) {
    marioConnection = conn;
    return conn.createChannel();
  });

  var smp = new SMP(self.participantIdentifier, nconf.get("username"), nconf.get("password"),  nconf.get("mariosbossurl"));

  return smp.getServiceMetadata(self.concept, self.action)
  .then(function(metadata) {
    return Promise.each(metadata, function(service, index, length){
      var q = service.ListenQueue;
      if(q && q!=="") {
        self.listeners.push(q);
        open.then(function(ch) {
          // LISTEN
          return ch.assertQueue(q).then(function(ok) {
            console.log("Listening to queue: " + q);
            return ch.consume(q, function(msg) {
              if (msg !== null) {
                var request;
                try {
                  request = JSON.parse(msg.content.toString());
                } catch (e) {
                  // TODO: What to do if content is not json???
                  ch.ack(msg);
                  return console.error(e);
                }

                //console.log(request);
                self.seneca.act(request, function (err, result) {
                  ch.ack(msg);
                  if (err) {
                    // TODO: check if there is no handler (Seneca act) and nack
                    console.error(err);
                  }

                  if(service.Usage !== "Provide") { return; }

                  var responseQueue = service.SendQueue;
                  var responseRoutingKey = service.SendRoutingKey;
                  if(!responseRoutingKey || responseRoutingKey === ""){ return; }

                  if(responseRoutingKey === "*"){
                    responseRoutingKey = msg.fields.routingKey;
                  }

                  console.log("Publishing to : " + service.SendExchange + " -> " + responseRoutingKey);
                  var response = self.envelopeMessage(err ? err : result.answer, request.concept, request.action, request, err);
                  ch.publish(service.SendExchange, responseRoutingKey, new Buffer(JSON.stringify(response)));
                });
              }
            });
          });
        });
      }

      if(service.Usage === "Consume"){
        console.log("Adding publisher to " + service.SendExchange + " -> " + service.SendRoutingKey);
        self.publishers.push(service.SendExchange + " -> " + service.SendRoutingKey);
        // Create Seneca server to handle requests
        self.seneca.add({usage: service.Usage, concept: service.ServiceConcept, action: service.ServiceAction}, function (msg, respond) {
          open.then(function(ch) {
            delete msg.meta$; // Remove Seneca metadata before publishing
            var request = self.envelopeMessage(msg.content, service.ServiceConcept, service.ServiceAction); // TODO - evaluate
            ch.publish(service.SendExchange, service.SendRoutingKey, new Buffer(JSON.stringify(request)));
            respond(null, request);
          });
        });
      }
    });
  });
};

module.exports = MarioConnect;
