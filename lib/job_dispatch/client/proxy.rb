# encoding: UTF-8

module JobDispatch

  # This is a simple class for making synchronous calls to the Job Queue dispatcher.
  class Client

    class Proxy

      attr :options

      def initialize(client, target, options={})
        @client = client
        @target = case target
                    when Class
                      target.to_s
                    when String
                      target
                    else
                      raise NotImplementedError, "Don't yet know how to serialize an object instance as a target"
                  end
        @options = options
      end

      def method_missing(method, *args)
        @client.enqueue(queue: queue, target: @target, method: method.to_s, parameters: args)
      end

      def queue
        @options[:queue] || :default
      end
    end
  end
end
