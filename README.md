# JobDispatch

[![Build Status](https://travis-ci.org/mobiledataanywhere/job_dispatch.png)](https://travis-ci.org/mobiledataanywhere/job_dispatch)

Job Dispatch is a gem for dispatching jobs to workers in an asynchronous manner. 
Job Dispatch does not require any specific database, deliberately separating the storage of jobs
from the dispatching of jobs to workers.
Jobs are dispatched using ZeroMQ messages and workers can be implemented in any language.

Only the dispatcher needs to access the database. This decouples the workers from the database.

## Installation

Add this line to your application's Gemfile:

    gem 'job_dispatch'

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install job_dispatch

## Usage

TODO: Write usage instructions here.

JobDispatch will work with any Job model class, provided it fulfils the following:

Attributes:

 * queue
 * status
 * target
 * method
 * enqueued_at
 * scheduled_at
 * expire_execution_at
 * completed_at
 * timeout
 * retry_count
 * retry_delay
 * result

Class Methods:

    dequeue_job_for_queue(queue, time=nil)

This method is for retrieving a single PENDING job from the database and atomically
marking it as being IN PROGRESS so that it can not be received again. It is the dispatcher's
Responsibility to update its status to completed/failed and schedule a retry if the
retry_count > 0.

See the example model classes in the examples/ directory.

## Contributing

1. Fork it
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create new Pull Request

## License

Licensed under MIT license, see LICENSE.txt.

