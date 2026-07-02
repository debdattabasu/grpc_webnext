The goal of this project is to create a grpc-webnext protocol that works in web browser amd support full grpc semantics. 

1. Unary rpc: Fetch
2. All streaming rpcs: Websocket.
3. Can serve either binary(content-type: @grpc-webnext/proto) od json(content-type: @application/json or @grpc-webnext/json)
4. websocket should send headers and trailers as protobuf mesages
5. fetch should send both headers and trailers as header. for this we should read whole response. should have configurable size limit.
6. existing protoc should work in both typescript and backend. 
7. Frontend api should mimic existing node grpc but need not be 1:1 match.
8. All connection management, retry, deadline etc semantics should be exactly same as standard grpc. All options should be supported exactly. 
9. Should be able to serve this protocol from same port as normal grpc. The content-type header disambiguates.
10. Option for using single websocket per stream or multiplexing all streams on websocket pool client side. Protocol should have websocket subscribe messages with stream id and headers and initial payload etc and should be able multiplex multiple streams on single websocket. We just optionally may choose not to use this feature which can also be siabled server side. 
11. Websocket multiplexing is strictly 1 message per websocket message and no fragmentation http2 style.

It should work over any http server. Default to http2+  we need: 

1. Rust backend library that can run standalone or wrap existing grpc server in rust.
2. Proxy in rust that can talk to any other grpc server at any endpoint.
3. Typescript client library that should work in browser and node etc also. 