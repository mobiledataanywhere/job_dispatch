module JobDispatch

  # This is a simple class for making synchronous calls to the Job Queue dispatcher.
  class Client
    def initialize(connect_address)
      @socket = JobDispatch.context.socket(ZMQ::REQ)
      @socket.connect(connect_address)
    end

    def send_request(command, options={})
      options[:command] = command
      @socket.send(JSON.dump(options))
      json = @socket.recv
      #puts "Received: #{json}"
      JSON.parse(json)
    end

    def method_missing(method, *args, ** kwargs)
      payload = kwargs
      payload[:parameters] = args
      send_request(method, payload)
    end

    def proxy_for(target)
      Proxy.new(self, target)
    end

    class Proxy
      def initialize(client, target)
        @client = client
        @target = case target
                    when Class
                      target.to_s
                    when String
                      target
                    else
                      raise NotImplementedError, "Don't yet know how to serialize an object instance as a target"
                  end
      end

      def method_missing(method, *args)
        @client.enqueue(job: {target: @target, method: method.to_s, parameters: args})
      end
    end
  end
end
