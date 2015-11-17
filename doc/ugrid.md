# Ugrid protocol

Motivation: define a simple, low overhead and fast protocol on top of TCP
or HTML5 websockets to perform publish/subscribe operations, remote procedure
calls, data streaming.

## Header format

* Destination index: unsigned integer, 32 bits, little endian
* Body size: unsigned integer, 32 bits, little endian

### Destination index

This field is set by client, and read by server to transmit a message
from one client to another. The client must obtain the value of
index corresponding to a remote from the server prior to send
messages to remote.

Reserved indexes:

* 0: __server__. This index is the server address.
* 1: __broadcast__. When this bit is set, the server sends the message
  to all connected clients.
* 2: __multicast__. When this bit is set, the server sends the message
  to all origin client subscribers.
* 3: __foreign__. This bit indicates that the message is coming from
  an external ugrid network (different ugrid server), and that the server has
  to read part of message content prior to transmit it (the body will start by
  a header).

### Body size

This fields indicate the number of bytes of the body following the header.
