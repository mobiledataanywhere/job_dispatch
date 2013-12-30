module JobDispatch
  class Broker
    #
    # This class represents a ZMQ socket where workers can phone in and ask for work to do.
    # If there is no work to do, an idle command is returned
    #
    # The response will be a single JSON encoded message such as:
    #
    # { "command":"idle" }
    #
    # or
    #
    # { "target":"MyMailer", "method":"deliver", "parameters":[14, "Subject", "Some other parameter"]}
    #
    # In which the worker should execute the code `MyMailer.deliver(14, 'Subject', 'Some other parameter')`
    #
    #
    # Other commands can be sent to the broker, such as:
    #
    # { "command":"status" } => returns status of the broker, number of idle workers per queue, etc.
    #
    class Socket
      attr_reader :socket

      def initialize(bind_address)
        @bind_address = bind_address
        @socket = nil
        @queues = {}
      end

      def connect
        unless @socket
          @socket = JobDispatch.context.socket(ZMQ::ROUTER)
          @socket.bind(@bind_address)
        end
        @socket
      end

      def disconnect
        if @socket
          @socket.close
          @socket = nil
        end
      end

      def poll_item
        @poll_item ||= ZMQ::Pollitem(@socket, ZMQ::POLLIN)
      end

      def readable?
        poller = ZMQ::Poller.new
        poller.register(poll_item)
        poller.poll(0) > 0
      end

      # Process a message received on the broker socket. The returned Command object contains the identity of the
      # requester (worker) so that replies can be sent to them.
      #
      # @return [Command]
      def read_command
        message = socket.recv_message #=> ZMQ::Message
        worker_id = Identity.new(message.unwrap.data.to_s.to_sym)
        json = message.first.data
        parameters = begin
          JSON.parse(json).with_indifferent_access
        rescue
          JobDispatch.logger.error("Received invalid json data: #{json.inspect} from socket id '#{worker_id}'")
          {error: "Invalid JSON"}
        end
        Command.new(worker_id, parameters)
      end

      # Send a command message to a worker.
      # @param [Command command] the command to send to the worker.
      def send_command(command)
        message = ZMQ::Message.new
        message.addstr(JSON.dump(command.parameters))
        message.wrap(ZMQ::Frame(command.worker_id.to_s))
        socket.send_message message
      end

    end
  end
end

