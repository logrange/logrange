# atmosphere - a simple log transport protocol
atmosphere is simple _synchronous request-response_ protocol for sending log data to an aggregation server. The _synchronous request-response_ term means that client will not send new data until it receives and recognizes response from server on its previous request.

Every wire message in both directions Client->Server or Server->Client is reprsented by the following format:
* data size (4 bytes) - 31 bit is used, so maximum payload size is 2Gib
* packet type (2 bytes) - 2 values are known 1 - _Auth_ packet, 2 - _Data_ packet
* data (various size) - data which is sent. Could be 0 length

There are 2 phases in Client-Server communication:
* Authentication
* Data exchange

## Authentication 
After connection with client server expects the client authentication. Only _Auth_ packets are accepted. Payload must be json in the following form:
```
{
    "ak": "testClient",
    "sk": "4xEp17eOvDzz1rjnjcWx4ay",
}
```
If server recognizes access-secret keys pair (ak, sk values), it responds by the following json in _Auth_ packet response:
```
{
    "sessId": "12384987349137249123749128374",
    "terminal": false
}
```
if the authentication was wrong, it will respond with json packet where `"terminal": true` and an explanation in `info` field. In case of failed authentication the server will close the connection shortly after the response.

## Data exchange
All data is sent using _Data_ packet type, which are acceptable for clients that passed authentication successfully. If a non-authenticated client sends the _Data_ packet the server will close the connection immediately. Any client's packet must be confirmed (even with 0 lenght response) by the server. So until client receives response on its packet, it cannot send next packet.

### Heart Beat
Heart beat is an authentication packet sent by Client to the server to let the server know, that client is alive. Server configuration can dictate to close connection in case of no _Auth_ packet from client is received for some period of time. Client should use session Id received from server while authentication process was run. New session id can be issued, so next time client has to use it.

# Examples
TBD.

