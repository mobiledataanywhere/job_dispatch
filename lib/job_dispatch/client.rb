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

    def close
      if @socket
        @socket.close
        @socket = nil
      end
    end

    # Enqueue a job to be processed describe by the passed job attributes.
    #
    # Required attributes:
    #   target: The target object that will execute the job. typically a class.
    #   method: the message to be sent to the target.
    # Optional:
    #   parameters: an array of parameters to be passed to the method.
    #   timeout: number of seconds after which the job is considered timed out and failed.
    def enqueue(job_attrs={})
      send_request('enqueue', {job: job_attrs})
    end

    # send a message to the dispatcher requesting to be notified when the job completes (or fails).
    def notify(job_id)
      send_request('notify', {job_id: job_id})
    end

    # as the dispatcher what was the last job enqueued on the given queue (or default)
    def last(queue=nil)
      job_or_raise send_request('last', {queue: queue||'default'})
    end

    # fetch the complete details for hte last job
    def fetch(job_id)
      job_or_raise send_request('fetch', {job_id: job_id})
    end

    private

    def job_or_raise(response)
      if response.is_a?(Hash) && response[:status] == 'success'
        response[:job]
      else
        p response
        raise ClientError, response[:message]
      end
    end
  end

  class ClientError < StandardError
  end
end

require 'job_dispatch/client/proxy'
require 'job_dispatch/client/synchronous_proxy'
