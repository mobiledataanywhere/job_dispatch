# job_dispatch change log

## Version 0.1.0

* Added 'num_tcp_connections' to status command response
* Fixed leaking sockets from job-worker command.

## Version 0.0.2

* Broker sends an idle command to a worker immediately upon connect. This helps recover from a case where a worker
  has been running for some time before the dispatcher starts. (Particularly with a Windows worker using NetMQ where
  a closed socket may still send messages.)
* Improve Ruby worker serialisation of exceptions into job result.

## Version 0.0.1

* First release
