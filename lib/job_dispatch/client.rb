# encoding: UTF-8

require 'active_support/core_ext/hash'

module JobDispatch

  # This is a simple class for making synchronous calls to the Job Queue dispatcher.
  class Client
    def initialize(connect_address=nil)
      @socket = JobDispatch.context.socket(ZMQ::REQ)
      @socket.connect(connect_address || JobDispatch.config.broker[:connect])
    end

    def send_request(command, options={})
      options[:command] = command
      @socket.send(JSON.dump(options))
      json = @socket.recv
      #puts "Received: #{json}"
      response = JSON.parse(json)
      response.is_a?(Hash) ? response.with_indifferent_access : response
    end

    def method_missing(method, *args, ** kwargs)
      payload = kwargs
      payload[:parameters] = args
      send_request(method, payload)
    end

    def proxy_for(target, options={})
      Proxy.new(self, target, options)
    end

    def synchronous_proxy_for(target, options={})
      SynchronousProxy.new(self, target, options)
    end

    def enqueue(job_attrs)
      send_request('enqueue', {job: job_attrs})
    end

    def notify(job_id)
      send_request('notify', {job_id: job_id})
    end

  end
end

require 'job_dispatch/client/proxy'
require 'job_dispatch/client/synchronous_proxy'
