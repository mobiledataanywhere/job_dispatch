# JobDispatch

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

## Contributing

1. Fork it
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create new Pull Request

## License

Licensed under MIT license, see LICENSE.txt.

