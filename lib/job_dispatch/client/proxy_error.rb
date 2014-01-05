# encoding: UTF-8

module JobDispatch

  # This is a simple class for making synchronous calls to the Job Queue dispatcher.
  class Client

    # When a proxy result is a failure, this exception is a class that will encapsulate the result.
    class ProxyError < StandardError
      attr :response

      def initialize(message, response=nil)
        @response = response
        super(message)
      end
    end
  end
end
