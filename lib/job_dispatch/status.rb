require 'text-table'

module JobDispatch
  class Status

    attr :socket


    def initialize(connect_address)
      @connect_address = connect_address
    end

    def connect
      if @socket.nil?
        @socket = JobDispatch.context.socket(ZMQ::REQ)
        @socket.connect(@connect_address)
      end
    end

    def disconnect
      @socket.close
      @socket = nil
    end

    def fetch
      @socket.send(JSON.dump({command:'status'}))
      json = @socket.recv
      @status = JSON.parse(json).with_indifferent_access
    end

    def print
      puts "Job Dispatcher status: #{@status[:status]}"
      puts ""

      table = Text::Table.new
      table.head = ['Queue', 'Worker ID', 'Worker Name', 'Status', 'Job ID', 'Job Details']
      table.rows = []

      @status[:queues].each do |queue, workers|
        if workers.empty?
          table.rows << [
              queue,
              '- no workers -',
              '',
              '',
              '',
              '',
          ]
        else
          workers.each_pair do |worker_id, worker_status|

            job = worker_status[:job]
            if job
              params_str = if job[:parameters]
                             job[:parameters].map(&:inspect).join(',')[0..20]
                           else
                             ''
                           end
              job_details = "#{job[:target]}.#{job[:method]}(#{params_str})"
            end

            table.rows << [
                queue,
                worker_id,
                worker_status[:name],
                worker_status[:status],
                worker_status[:job_id],
                job_details,
            ]
          end
        end
      end

      puts table.to_s
    end

  end
end
