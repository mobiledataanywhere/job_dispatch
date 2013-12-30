module JobDispatch

  # This class represents a ZeroMQ socket for signalling to the broker that there are jobs immediately available.
  class Signaller
    attr :socket

    def initialize(wakeup_connect_address)
      @wakeup_connect_address = wakeup_connect_address
    end

    def connect
      if @socket.nil?
        @socket = JobDispatch.context.socket(ZMQ::PUB)
        @socket.connect(@wakeup_connect_address)
      end
    end

    def disconnect
      if @socket
        @socket.close
        @socket = nil
      end
    end

    # signals are a straight
    def signal(queue='default')
      @socket.send(queue)
    end
  end
end
