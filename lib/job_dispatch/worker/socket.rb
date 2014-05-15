# encoding: UTF-8

require 'json'

module JobDispatch

  class Worker
    class Socket

      attr :socket
      attr :touch_socket
      attr :item_class

      def initialize(connect_address, item_klass)
        @socket = JobDispatch.context.socket(ZMQ::REQ)
        @socket.connect(connect_address)
        @touch_socket = JobDispatch.context.socket(ZMQ::DEALER)
        @touch_socket.connect(connect_address)
        @item_class = item_klass
      end

      def poll_item
        @poll_item ||= ZMQ::Pollitem(@socket, ZMQ::POLLIN)
      end

      def ask_for_work(queue)
        @socket.send(JSON.dump({command: 'ready', queue: queue, worker_name: identity}))
      end

      def send_goodbye(queue)
        @socket.send(JSON.dump({command: 'goodbye', worker_name: identity}))
      end

      def close
        @socket.close
      end

      def identity
        @identity ||= begin
          hostname = ::Socket.gethostname
          process = Process.pid
          thread = Thread.current.object_id.to_s(16)
          ['ruby', hostname, process, thread].join(':')
        end
      end

      # read an incoming message. The thread will block if there is no readable message.
      #
      # @return [JobDispatch::Item] the item to be processed (or nil if there isn't a valid job)
      def read_item
        begin
          drain_touch_socket
          json = @socket.recv
          params = JSON.parse(json)
          case params["command"]
            when "job"
              item = item_class.new params["target"], params["method"], *params["parameters"]
            when "idle"
              item = item_class.new "JobDispatch", "idle"
            when "quit"
              puts "It's quittin' time!"
              Process.exit(0)
            else
              item = item_class.new "JobDispatch", "unknown_command", params
          end
          item.job_id = params["job_id"]
        rescue StandardError => e
          JobDispatch.logger.error "Failed to read message from worker socket: #{e}"
          nil
        end
        item
      end

      # drain any messages that may have been received on the touch socket.
      def drain_touch_socket
        loop do
          message = @touch_socket.recv_nonblock
          break if message.nil?
        end
      end

      # after execution, send the response.
      def send_response(job_id, status, result)
        JobDispatch.logger.info "Worker #{Process.pid} completed job_id: #{job_id}: #{status}, result: #{result}"
        response = {
            command: 'completed',
            ready: true,
            job_id: job_id,
            result: result,
            status: status
        }
        @socket.send(JSON.dump(response))
      end

      def send_touch(job_id, timeout=nil)
        hash = {
            command: 'touch',
            job_id: job_id
        }
        hash[:timeout] = timeout if timeout
        @touch_socket.send(JSON.dump(hash))
      end
    end
  end
end
