require "job_dispatch/version"

#require 'job_dispatch/broker'

require 'active_support/dependencies/autoload'
require 'active_support/core_ext/hash/indifferent_access'
require 'active_support/core_ext/module/attribute_accessors'
require 'nullobject'
require 'rbczmq'

module JobDispatch

  extend ActiveSupport::Autoload

  autoload :Broker
  autoload :Configuration
  autoload :Job
  autoload :Status
  autoload :Worker
  autoload :Identity

  def configure(&block)
    Configuration.configure(&block)
  end

  def config
    Configuration.config
  end

  # @return [ZMQ::Context] return or create a ZeroMQ context.
  def context
    ZMQ.context || ZMQ::Context.new
  end

  def idle
    "idle, doing nothing"
  end

  def unknown_command(params)
    puts "Unknown command: #{params.inspect}"
  end

  # This signals to the job broker(s) that there are jobs immediately available on the given queue.
  def signal(queue='default')
    self.signaller ||= if config.signaller && config.signaller[:connect]
                         signaller = JobDispatch::Signaller.new(config.signaller[:connect])
                         signaller.connect
                         signaller
                       else
                         Null::Object.instance
                       end
    self.signaller.signal(queue)
  end


  def enqueue(job_attrs)
    address = JobDispatch.config.broker[:connect]
    socket = JobDispatch.context.socket(ZMQ::REQ)
    socket.connect(address)
    socket.send(JSON.dump({command:'enqueue',job:job_attrs}))
    result = JSON.parse(socket.recv)
    socket.close
    result
  end

  module_function :context
  module_function :idle
  module_function :unknown_command
  module_function :signal
  module_function :configure
  module_function :config
  module_function :enqueue

  mattr_accessor :logger
  mattr_accessor :signaller
end

JobDispatch.logger = Null::Object.instance
