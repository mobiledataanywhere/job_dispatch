# encoding: UTF-8

require 'set'

module JobDispatch

  # The broker is the central communications service of JobDispatch. Clients and Workers both connect
  # to a ZeroMQ ROUTER socket. Clients and workers use a REQ socket, and send a request. The Broker
  # sends a reply immediately or at some point in the future when it is appropriate (eg: when there)
  # is a job to do for a worker, or when a job is completed for a client waiting on job notification).
  class Broker

    WORKER_IDLE_TIME = 10
    POLL_TIME = 5
    STOP_SIGNALS = %w[INT TERM KILL]

    IdleWorker = Struct.new :worker_id, :idle_since, :queue, :worker_name


    # any object that will respond to `next_job_for_queue`, which should return a job, or nil if there
    # are no jobs for that queue. The returned job should be a JSONable object that will be sent to the worker.
    # This should include `target`, `action` and `parameters` keys.
    attr :socket
    attr :workers_waiting_for_reply # Array of Identity
    attr :workers_waiting_for_jobs # Hash of key: Identity, value: IdleWorker
    attr :worker_names # Hash of key: Identity actual ZMQ identity, value: String claimed identity
    attr :jobs_in_progress
    attr :jobs_in_progress_workers
    attr :queues
    attr_accessor :verbose
    attr :status
    attr :job_subscribers # Key: job_id, value: list of Socket Identities waiting for job completion notifications.
    attr :pub_socket
    attr_accessor :reply_exceptions

    def initialize(worker_bind_address, wakeup_bind_address, publish_bind_address=nil)
      @worker_bind_address = worker_bind_address
      @wakeup_bind_address = wakeup_bind_address
      @publish_bind_address = publish_bind_address

      # to track REQ-REP state:
      @workers_waiting_for_reply = [] # array of Symbol (worker id = zmq identity of worker)

      # to track jobs:
      @workers_waiting_for_jobs = {} # Hash of key: Identity(worker_id) value: IdleWorker
      @queues = Hash.new { |hash, key| hash[key] = Set.new } # key:queue name, value: Array of Identity of worker id
      @jobs_in_progress = {} # key: job_id, value: Job model object
      @jobs_in_progress_workers = {} #key: job_id, value: worker_id
      @worker_names = {} # Key: Symbol socket identity, value: String claimed name of worker
      @job_subscribers = {} # Key: job_id, value: list of Socket Identities waiting for job completion notifications.
      @status = "OK"
      @reply_exceptions = true

      queues[:default] # ensure the default queue exists.
    end

    def running?
      @running
    end

    def verbose?
      verbose
    end

    def run
      begin
        puts "JobDispatch::Broker running in process #{Process.pid}"
        JobDispatch.logger.info("JobDispatch::Broker running in process #{Process.pid}")
        @running = true
        poller = ZMQ::Poller.new

        @socket = JobDispatch::Broker::Socket.new(@worker_bind_address)
        @socket.connect
        poller.register(@socket.poll_item)

        if @publish_bind_address
          @pub_socket = JobDispatch.context.socket(ZMQ::PUB)
          @pub_socket.bind(@publish_bind_address)
        end

        if @wakeup_bind_address
          JobDispatch.logger.info("JobDispatch::Broker signaller SUB socket bound to #{@wakeup_bind_address}")
          @wake_socket = JobDispatch.context.socket(ZMQ::SUB)
          @wake_socket.subscribe('')
          @wake_socket.bind(@wakeup_bind_address)
          poller.register(@wake_socket)
        end

        while running?
          begin
            process_messages(poller)
            dispatch_jobs_to_workers
            expire_timed_out_jobs
            send_idle_commands
          rescue SignalException => e
            signal_name = Signal.signame(e.signo)
            if STOP_SIGNALS.include?(signal_name)
              JobDispatch.logger.info("JobDispatch::Broker shutting down, due to #{signal_name} signal")
              puts "JobDispatch::Broker shutting down, due to #{signal_name} signal"
              @running = false
              @status = "SHUTDOWN"
              sleep 1
              process_quit
              sleep 1
            end
          end
        end
      ensure
        @socket.disconnect if @socket
        @socket = nil
      end
    end


    def process_messages(poller)
      poller.poll(POLL_TIME)

      if @wake_socket && poller.readables.include?(@wake_socket)
        @wake_socket.recv # no message to process, just consume messages in order to wake the poller
      end

      if poller.readables.include?(socket.socket)
        command = read_command
        reply = process_command(command)
        send_command(reply) if reply
      end
    end


    # read a command from a worker. We will keep a 1:1 REQ-REP model with each worker so we need to track the
    # state of the worker.
    def read_command
      command = socket.read_command
      @workers_waiting_for_reply << command.worker_id
      command
    end


    # send a command out the socket. Also maintains the state of the list of workers so that we can keep the
    # REQ-REP contract.
    def send_command(command)
      raise "Worker not waiting for reply" unless workers_waiting_for_reply.include?(command.worker_id)
      workers_waiting_for_reply.delete(command.worker_id)
      JobDispatch.logger.debug("sending command: #{command.inspect}")
      socket.send_command command
    end


    def process_command(command)
      # prepare for immediate reply
      reply = Broker::Command.new(command.worker_id)

      begin
        case command.command
          when "ready"
            # add to list of workers who are ready for work
            add_available_worker(command)

            # don't reply, leaves worker blocked waiting for a job to do.
            reply = nil

          when "goodbye"
            reply.parameters = remove_available_worker(command)

          when "completed"
            #  process completed job.
            handle_completed_job(command)

            if command.worker_ready?
              # a completed job also means the worker is available for more work.
              add_available_worker(command)
              reply = nil
            else
              reply.parameters = {:status => 'thanks'}
            end

          when "notify"
            # synchronous notification of job status.

            job_id = command.parameters[:job_id]
            raise MissingParameterError, "Missing 'job_id' parameter" unless job_id

            if jobs_in_progress[job_id]
              workers_waiting_for_reply << command.worker_id
              job_subscribers[job_id.to_s] ||= []
              job_subscribers[job_id.to_s] << command.worker_id
              reply = nil
            else
              job = job_source.find(job_id) # load job from storage and return to requester.
              reply.parameters = job_status_parameters(job)
            end


          when "touch"
            # perhaps this could also be processed of a PUB/SUB socket so that it doesn't require a synchronous
            # response to the worker...
            reply.parameters = touch_job(command)

          when "status"
            reply.parameters = status_response

          when "enqueue"
            reply.parameters = create_job(command)

          when "quit"
            process_quit
            reply.parameters = {:status => 'bye'}
            @running = false

          else
            # unknown command, reply with error immediately to fulfil REQ-REP state machine contract.
            reply.parameters = {:status => 'unknown command!'}
        end

      rescue RSpec::Expectations::ExpectationNotMetError
        raise # allow test exceptions through.
      rescue StandardError => e
        if reply_exceptions
          # all others reply over socket.
          JobDispatch.logger.error(e.to_s)
          reply.parameters = {:status => 'error', :message => e.to_s}
        else
          # used during testing to raise errors so that Rspec can catch them as a test failure.
          raise
        end
      end

      reply
    end

    def send_idle_commands(idle_time=nil)
      idle_time ||= Time.now
      idle_time -= WORKER_IDLE_TIME
      idle_workers = @workers_waiting_for_jobs.select { |worker_id, worker| worker.idle_since < idle_time }
      idle_workers.each do |worker_id, worker|
        send_job_to_worker({:command => 'idle'}, worker_id)
      end
    end


    def send_job_to_worker(hash, worker_id)
      # remove from queue and idle workers lists.
      idle_worker = @workers_waiting_for_jobs.delete(worker_id)
      @queues[idle_worker.queue].delete(worker_id)
      # send the command.
      command = Broker::Command.new(worker_id, hash)
      JobDispatch.logger.info("Sending command '#{hash[:command]}' to worker: #{worker_id.to_json}")
      send_command(command)
    end


    # add a worker to the list of workers available for jobs.
    def add_available_worker(command)
      JobDispatch.logger.info("Worker '#{command.worker_id.to_json}' available for work on queue '#{command.queue}'")
      queue = command.queue
      idle_worker = IdleWorker.new(command.worker_id, Time.now, queue, command.worker_name)
      workers_waiting_for_jobs[command.worker_id] = idle_worker
      queues[queue] << command.worker_id
      if command.worker_name # this is only sent on initial requests.
        worker_names[command.worker_id] = command.worker_name
      end
    end

    # remove a worker from available list. Worker is shutting down or indicating that it will no longer
    # be available for doing work.
    def remove_available_worker(command)
      JobDispatch.logger.info("Worker '#{command.worker_id.to_json}' available for work on queue '#{command.queue}'")

      # the goodbye command is sent by another socket connection, so the worker_id (socket identity) will
      # not match the socket actually waiting for work.

      keys = worker_names.select { |id, name| name == command.worker_name }.keys
      keys.each do |worker_id|
        workers_waiting_for_reply.delete(worker_id) # socket will be closing, no need to send it anything.
        worker = workers_waiting_for_jobs.delete(worker_id)
        queues[worker.queue].delete(worker_id) if worker
        worker_names.delete(worker_id)
      end

      {status: "see ya later"}
    end

    def dispatch_jobs_to_workers
      # dequeue jobs from database for each queue
      @queues.each_pair do |queue, worker_ids|
        # we only need to check the database if there are available workers in that queue
        if worker_ids.count > 0
          worker_id = worker_ids.first
          job = job_source.dequeue_job_for_queue(queue.to_s)
          if job
            hash = json_for_job(job)

            # use the job record id or assign a uuid as the job id
            job_id = (job.id || SecureRandom.uuid).to_s
            hash[:job_id] = job_id
            hash[:command] = 'job'

            JobDispatch.logger.info("dispatching job #{job_id} to worker #{worker_id.to_json}")
            send_job_to_worker(hash, worker_id)

            job.expire_execution_at = Time.now + (job.timeout || Job::DEFAULT_EXECUTION_TIMEOUT)
            job.status = JobDispatch::Job::IN_PROGRESS
            job.save

            publish_job_status(job)

            @jobs_in_progress[job_id] = job
            @jobs_in_progress_workers[job_id] = worker_id
          end
        end
      end
    end


    def expire_timed_out_jobs
      expired_job_ids = @jobs_in_progress.each_with_object([]) do |(job_id, job), expired|
        # check if job has timed out. If so, implement retry logic.
        expired << job_id if job.timed_out?
      end

      expired_job_ids.each do |job_id|
        job = jobs_in_progress.delete(job_id)
        @jobs_in_progress_workers.delete(job_id)
        if job
          job.failed!("job timed out")
          publish_job_status(job)
        end
      end
    end

    def queues_with_available_workers
      @queues.each_with_object([]) do |(queue, workers), object|
        object << queue unless workers.nil? || workers.empty?
      end
    end


    def handle_completed_job(command)
      # look up the job and process its completion.
      job_id = command.parameters[:job_id]
      if job_id
        job = jobs_in_progress.delete(job_id)
        @jobs_in_progress_workers.delete(job_id)
        if job
          job.reload
          JobDispatch.logger.info(
              "completed job #{job_id} from worker #{command.worker_id.to_json} status = #{command.parameters[:status]}")
          if command.success?
            job.succeeded!(command.parameters[:result])
            publish_job_status(job)
          else
            job.failed!(command.parameters[:result])
            publish_job_status(job)
          end
        else
          JobDispatch.logger.error("Job #{job_id} completed, but does not exist. Probably timed out!")
          raise "No job"
        end
      end
    end

    def process_quit
      JobDispatch.logger.info("Sending quit message to idle workers")

      quit_params = {command: 'quit'}
      until workers_waiting_for_jobs.empty?
        worker_id = @workers_waiting_for_jobs.first.first
        send_job_to_worker(quit_params, worker_id)
      end
    end


    def json_for_job(job)
      hash = if job.respond_to? :as_job_queue_item
               job.as_job_queue_item
             else
               job.as_json
             end.with_indifferent_access
      hash[:id] = hash[:id].to_s
      hash
    end


    def status_response
      response = {
          :status => status,
          :queues => {}
      }

      queues.each_pair do |queue, workers|
        response[:queues][queue] = {}
      end

      jobs_in_progress.each_with_object(response[:queues]) do |(job_id, job), _queues|
        queue = job.queue
        _queues[queue] ||= {}
        worker_id = jobs_in_progress_workers[job_id]
        _queues[queue][worker_id.to_hex] = {
            :status => :processing,
            :name => worker_names[worker_id],
            :job_id => job_id,
            :queue => job.queue,
            :job => json_for_job(job),
        }
      end

      workers_waiting_for_jobs.each_with_object(response[:queues]) do |(worker_id, worker), _queues|
        queue = worker.queue
        _queues[queue] ||= {}
        _queues[queue][worker_id.to_hex] = {
            :status => :idle,
            :name => worker_names[worker_id],
            :queue => worker.queue,
        }
      end

      response
    end

    # reset the timeout on the job. Called for a long process to confirm to the dispatcher that the worker is
    # still actively working on the job and has not died.
    #
    # @return [Hash] result to be sent to client.
    def touch_job(command)
      job_id = command.parameters[:job_id]
      timeout = command.parameters[:timeout] || Job::DEFAULT_EXECUTION_TIMEOUT
      job = @jobs_in_progress[job_id]
      if job
        job.expire_execution_at = Time.now + timeout
        JobDispatch.logger.info("JobDispatch::Broker#touch timeout on job #{job_id} to #{job.expire_execution_at}")
        job.save
        {status: "success"}
      else
        {status: "error", message: "the specified job does not appear to be in progress"}
      end
    end

    def create_job(command)
      begin
        raise MissingParameterError, "Missing 'job' from command" unless command.parameters[:job].present?

        job_attrs = command.parameters[:job]
        job = job_source.create!(job_attrs)
        {status: 'success', job_id: job.id.to_s}
      rescue StandardError => e
        JobDispatch.logger.error "JobDispatch::Broker#create_job error: #{e}"
        {status: 'error', message: e.to_s}
      end
    end

    private

    def job_source
      JobDispatch.config.job_class
    end

    def publish_job_status(job)
      parameters = job_status_parameters(job)

      if pub_socket
        # send as plain text so that ZMQ SUB filtering can be done on the job_id.
        # sent as two lines: job_id then LF then status.
        pub_socket.send("#{job.id}\n#{parameters[:status]}")
      end

      socket_ids = job_subscribers.delete(job.id.to_s)
      if socket_ids
        socket_ids.each do |socket_id|
          # send the command.
          command = Broker::Command.new(socket_id, parameters)
          JobDispatch.logger.info("Sending job notification for job id '#{job.id}' status = #{status} to socket: #{socket_id.to_json}")
          send_command(command)
        end
      end
    end

    def job_status_parameters(job)
      {
          status: Job::STATUS_STRINGS[job.status] || 'unknown',
          job_id: job.id.to_s,
          job: json_for_job(job)
      }
    end

    class MissingParameterError < StandardError
    end
  end
end

require 'job_dispatch/broker/command'
require 'job_dispatch/broker/socket'
