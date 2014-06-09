# encoding: UTF-8

module JobDispatch

  #
  # This class is the main worker loop. Run it as a whole process or just as a thread in a multi-threaded worker
  # process.
  #
  class Worker

    class StopError < StandardError
    end

    IDLE_TIME = 3
    IDLE_COUNT = 10

    attr :socket
    attr :queue
    attr :item_class

    def initialize(connect_address, options={})
      options ||= {}
      @connect_address = connect_address
      @queue = options[:queue] || 'default'
      @running = false
      @item_class = options[:item_class] || Worker::Item
    end

    def connect
      @socket ||= Worker::Socket.new(@connect_address, item_class)
      Thread.current["JobDispatch::Worker.socket"] = @socket
    end

    def disconnect
      if @socket
        @socket.close
        @socket = nil
        Thread.current["JobDispatch::Worker.socket"] = nil
      end
    end

    def run
      @running = true
      @running_thread = Thread.current
      while running?
        # puts "connecting"
        connect
        # puts "asking for work"
        ask_for_work rescue StopError

        # if we are idle for too many times, the broker has restarted or gone away, and we will be stuck in receive
        # state, so we need to close the socket and make a new one to ask for work again.

        idle_count = 0
        poller = ZMQ::Poller.new
        poller.register(socket.poll_item)
        while running? and idle_count < IDLE_COUNT
          begin
            poller.poll(IDLE_TIME)
            if poller.readables.include?(socket.socket)
              process
              idle_count = 0
            else
              idle
              idle_count += 1
            end
          rescue Interrupt, StopError
            JobDispatch.logger.info("Worker stopping.")
            stop
            disconnect
            # Tell the broker goodbye so that we are removed from the idle worker list and no more jobs will come here.
            connect
            send_goodbye
            sleep(0.1) # let the socket send the message before we disconnect...
          end
        end
        disconnect
      end
    end

    def ask_for_work
      socket.ask_for_work(queue)
    end

    def send_goodbye
      socket.send_goodbye(queue)
    end

    def running?
      @running
    end

    def stop
      if running?
        @running_thread.raise StopError unless @running_thread == Thread.current
        @running = false
      end
    end

    def self.touch(timeout=nil)
      sock = Thread.current["JobDispatch::Worker.socket"]
      job_id = Thread.current["JobDispatch::Worker.job_id"]
      if sock && job_id
        sock.send_touch(job_id, timeout)
        JobDispatch.logger.debug { "touching job #{job_id}"}
      end
    end

    private

    # called when the socket is readable. do some work.
    def process
      item = @socket.read_item
      if item
        item.execute
        @socket.send_response(item.job_id, item.status, item.result)
      else
        @socket.send_response("unknown", :error, "failed to decode command")
      end
    end

    def idle
      # puts "waiting for job to do…"
    end
  end
end

require 'job_dispatch/worker/socket'
require 'job_dispatch/worker/item'
