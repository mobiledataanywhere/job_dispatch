require 'json'

module JobDispatch

  class Worker
    class Socket

      attr :socket

      def initialize(connect_address)
        @socket = JobDispatch.context.socket(ZMQ::REQ)
        @socket.connect(connect_address)
      end

      def poll_item
        @poll_item ||= ZMQ::Pollitem(@socket, ZMQ::POLLIN)
      end

      def ask_for_work(queue)
        puts "Asking for work on queue: #{queue}"
        @socket.send(JSON.dump({command: 'ready', queue: queue, worker_name: identity}))
      end

      def send_goodbye(queue)
        puts "Sending goodbye to broker."
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
        json = @socket.recv
        begin
          puts "Received: #{json}"
          params = JSON.parse(json)
          case params["command"]
            when "job"
              item = Item.new params["target"], params["method"], *params["parameters"]
            when "idle"
              item = Item.new "JobDispatch", "idle"
            when "quit"
              puts "It's quittin' time!"
              exit(0)
            else
              item = Item.new "JobDispatch", "unknown_command", params
          end
          item.job_id = params["job_id"]
        rescue StandardError => e
          puts "Failed to read message from worker socket: #{e}"
          nil
        end
        puts "Received item of work to do: #{item.inspect}"
        item
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
        @socket.send(JSON.dump(hash))
        json = @socket.recv # wait for acknowledgement... this could be done via pub/sub to be asynchronous.
        JSON.parse(json) rescue {:error => "Failed to decode JSON from dispatcher: #{json}"}
      end

    end
  end
end