module JobDispatch
  class Status

    attr :socket


    def initialize(connect_address)
      @connect_address = connect_address
    end

    def connect
      if @socket.nil?
        @socket = JobDispatch.context.socket(ZMQ::REQ)
        @socket.connect('tcp://127.0.0.1:5555')
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
      puts "%-16s %s" % ['Queue', 'Status']
      puts "%-16s %s" % ['-----', '------']
      @status[:queues].each do |queue, workers|
        puts "%-16s %6d idle worker(s):" % [queue, workers.count]
        workers.each_with_index do |worker, index|
          puts "                        #%d %s" % [index, worker]
        end
      end

      puts ""
      puts "Jobs in progress:"
      puts ""


      column_widths = [0,0,0,0]
      table = []

      @status[:in_progress].each_pair do |job_id, job|
        params_str = if job[:job][:parameters]
                       job[:job][:parameters].map(&:inspect).join(',')[0..20]
                     else
                       ''
                     end
        desc = "#{job[:job][:target]}.#{job[:job][:method]}(#{params_str})"
        row = [job_id,  job[:worker_id], job[:job][:expire_execution_at], desc[0..40]]
        row.each_with_index do |string,index|
          column_widths[index] = [column_widths[index], string.length].max
        end
        table << row
      end

      format = column_widths.map{|w| "%-#{w}s"}.join('  ')
      puts format % ['JobId', 'Worker', 'Expires', 'Details']
      puts format % ['-----', '------', '-------', '-------']

      table.each {|row| puts format % row }

      if @status[:in_progress].empty?
        puts " -- no jobs in progress"
      end

    end

  end
end
