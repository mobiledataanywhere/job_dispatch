module JobDispatch::Sockets
  class Enqueue
    def initialize(bind_address)
      @socket = JobDispatch.context.socket(ZMQ::REQ)
      @socket.bind(bind_address)
    end

    def poll_item
      @poll_item ||= ZMQ::Pollitem(@socket, ZMQ::POLLIN)
    end

    # Enqueue socket when it receives a message simply stores it in the database.
    # It will also send a message to wake a connected dispatcher
    def process

    end
  end
end
