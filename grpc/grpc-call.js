module.exports = function (RED) {
    "use strict";
    const grpc = require("@grpc/grpc-js");
    const getByPath = require('lodash.get');
    const utils = require('../utils/utils');
    const fs = require("fs");

    function gRpcCallNode(config) {
        try {
            const node = this;
            RED.nodes.createNode(node, config);

            // Get the gRPC server from the server config Node
            const serverNode = RED.nodes.getNode(config.server)
            node.on("input", function (msg) {
                // Overriding config with msg content
                config.service = config.service || msg.service;
                config.method = config.method || msg.method;
                
                try {
                    const REMOTE_SERVER = serverNode.server + ":" + serverNode.port;
                    // Create gRPC client
                    let proto =  serverNode.proto;
                    if (serverNode.protoPackage) {
                        proto = getByPath(serverNode.proto, serverNode.protoPackage);
                    }
                    if (!proto[config.service]) {
                        node.status({fill:"red",shape:"dot",text: "Service " + config.service + " not in proto file"});
                    } else if (!proto[config.service].service[config.method]) {
                        node.status({fill:"red",shape:"dot",text: "Method " + config.method + " not in proto file for service " +  config.service });
                    } else {
                        node.status({});
                        node.chain = config.chain;
                        node.key = config.key;

                        let credentials;
                        if (serverNode.ssl){
                            if (!serverNode.selfsigned){
                                credentials = grpc.credentials.createSsl();
                            } else if (serverNode.caPath){
                                if (serverNode.mutualTls){
                                    const chain = utils.tempFile('cchain.txt', node.chain)
                                    const key = utils.tempFile('ckey.txt', node.key)
                        
                                    credentials = grpc.credentials.createSsl(
                                        fs.readFileSync(serverNode.caPath),
                                        fs.readFileSync(key),
                                        fs.readFileSync(chain),
                                    );
                                } else {
                                    credentials = grpc.credentials.createSsl(
                                        fs.readFileSync(serverNode.caPath),
                                    );
                                }
                            }
                        }
                        
                        const metadata = new grpc.Metadata();
                        if (typeof msg.metadata === 'object'){
                            Object.keys(msg.metadata).forEach(key => {
                                try {
                                    metadata.add(key, JSON.stringify(msg.metadata[key]));
                                } catch (e){
                                    node.error(e);
                                }
                            });
                        }

                        if (node.client) {
                            if (!node.client[config.method]) {
                                node.status({fill:"red",shape:"dot",text: "Method " + config.method + " not in proto file"});
                            } else {
                                node.status({});
                                if (proto[config.service].service[config.method].responseStream) {
                                    node.channel = node.client[config.method](msg.payload, metadata);
                                    let http2Content = ''; // Accumulate HTTP/2 content
                                    let headers = {}; // Store response headers
                                    let trailers = {}; // Store response trailers
                                    let statusCode = null; // Store response status code
                                    
                                    // Event: Data
                                    node.channel.on("data", function (data) {
                                        http2Content += data; // Accumulate received data
                                        // If you want to send each chunk of data separately, send it here
                                    });

                                    // Event: Headers
                                    node.channel.on("status", function (status) {
                                        statusCode = status.code; // Store status code
                                        headers = status.metadata.getMap(); // Store response headers
                                    });

                                    // Event: Trailers
                                    node.channel.on("end", function () {
                                        // HTTP/2 content received completely, send out the response
                                        const response = {
                                            statusCode: statusCode,
                                            headers: headers,
                                            trailers: trailers,
                                            body: http2Content
                                        };
                                        msg.payload = response;
                                        msg.metadata = metadata; // Pass metadata along with the response payload
                                        node.send(msg);
                                    });

                                    node.channel.on("error",function (error) {
                                        msg.error = error;
                                        node.send(msg);
                                    });

                                } else {
                                    node.client[config.method](msg.payload, metadata, function(error, data, responseMetadata) {
                                        const response = {
                                            statusCode: responseMetadata.status.code,
                                            headers: responseMetadata.metadata.getMap(),
                                            trailers: responseMetadata.trailers,
                                            body: data
                                        };
                                        msg.payload = response;
                                        msg.metadata = metadata; // Pass metadata along with the response payload
                                        msg.error = error;
                                        node.send(msg);
                                    })
                                }
                            }
                        } else {
                            node.error("gRPC client is not initialized.");
                        }

                    }

                } catch (err) {
                    node.error("Error occurred: " + err);
                }

            });

            node.on("error", function (error) {
                node.error("gRpcCallNode Error - " + error);
                console.log(error);
            });

            node.on("close", function (done) {
                if (node.client) {
                    grpc.closeClient(node.client)
                    delete node.client;
                    delete node.channel
                }
                done();
            });
        } catch (err) {
            node.error("gRpcCallNode" + err);
            console.log(err);
        }
    }

    RED.nodes.registerType("grpc-call", gRpcCallNode);
};
