# NSQ to Kinesis 

Take records from an NSQ topic, put them in Kinesis.

Works, but still WIP; needs much polishing.

## Usage

For manual testing with kinesalite and nsqd:

```sh
./nsq2kinesis -kinesis-endpoint http://localhost:4567 -stream <stream> -topic <topic> -make-stream 
```
