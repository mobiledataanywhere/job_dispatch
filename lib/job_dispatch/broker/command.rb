require 'json'

module JobDispatch
  # A very simple representation of a worker and a message received from or to be sent to that worker.
  # this represents job messages from the Broker's ROUTER socket perspective. ie: messages are wrapped
  # by the worker ID so that they can be routed to the client.
  #
  # The parameters object is expected to be a Hash with indifferent access
  class Broker
    class Command
      attr_accessor :worker_id
      attr_accessor :parameters

      def initialize(worker_id=nil, parameters=nil)
        @worker_id, @parameters = worker_id, parameters
      end

      def worker_ready?
        @parameters[:command] == "ready" || @parameters[:ready]
      end

      def queue
        (@parameters[:queue] || :default).to_sym
      end

      def command
        @parameters[:command]
      end

      def status
        @parameters[:status] && @parameters[:status].to_sym
      end

      def success?
        status == :success
      end

      # the name of the worker
      def worker_name
        @parameters[:worker_name]
      end
    end
  end

end
