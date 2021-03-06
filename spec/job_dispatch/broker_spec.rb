require 'rspec'
require 'spec_helper'

# dummy Job class
Job = TestJob

describe JobDispatch::Broker do

  Command ||= JobDispatch::Broker::Command
  Identity ||= JobDispatch::Identity

  subject { JobDispatch::Broker.new('tcp://localhost:2000', 'tcp://localhost:2001') }

  let(:worker_id) { Identity.new([0, 0x80, 0, 0x41, 0x31].pack('c*')) }
  let(:worker_id2) { Identity.new([0, 0x80, 0, 0x41, 0x32].pack('c*')) }
  let(:worker_id3) { Identity.new([0, 0x80, 0, 0x41, 0x33].pack('c*')) }

  before :each do
    JobDispatch.config.job_class = double('JobClass')
  end

  context "tracking communication state" do

    it "reading a command adds the requester to the list of connections awaiting reply" do
      command = Command.new(worker_id, {commmand: 'ready'})
      socket = double('Broker::Socket', :read_command => command)
      subject.stub(:socket => socket)
      subject.read_command
      expect(subject.workers_waiting_for_reply).to include(worker_id)
    end

    it "sending a command removes the worker from the list of workers awaiting reply" do
      subject.workers_waiting_for_reply << worker_id
      command = Command.new(worker_id, {commmand: 'idle'})
      socket = double('Broker::Socket', :send_command => command)
      socket.should_receive(:send_command).with(command)
      subject.stub(:socket => socket)
      subject.send_command(command)
      expect(subject.workers_waiting_for_reply).not_to include(worker_id)
    end

    it "does not send a command to a worker unless it is awaiting a reply" do
      command = Command.new(worker_id, {commmand: 'idle'})
      socket = double('Broker::Socket', :send_command => command)
      socket.should_not_receive(:send_command)
      subject.stub(:socket => socket)
      expect { subject.send_command(command) }.to raise_error
    end
  end

  context "responding to command" do

    context "'status'" do
      let(:command) { Command.new(worker_id, {command: 'status'}) }

      before :each do
        subject.reply_exceptions = false
      end

      it "returns a command" do
        expect(subject.process_command(command)).to be_a(Command)
      end

      it "returns a status object" do
        result = subject.process_command(command)
        expect(result.worker_id).to eq(worker_id)
        expect(result.parameters).to be_a(Hash)
        expect(result.parameters[:status]).to eq('OK')
        expect(result.parameters[:queues]).to be_a(Hash)
      end

      it "returns a JSONable parameters object" do
        subject.process_command(Command.new(worker_id2, {command: 'ready', worker_name: 'test worker'}))
        result = subject.process_command(command)
        expect { json = JSON.dump(result.parameters) }.not_to raise_error
      end

      it "returns a list of workers including idle and working" do
        subject.workers_waiting_for_reply << worker_id2
        subject.process_command(Command.new(worker_id2, {command: 'ready', worker_name: 'test worker 1'}))
        subject.workers_waiting_for_reply << worker_id3
        subject.process_command(Command.new(worker_id3, {command: 'ready', worker_name: 'test worker 2'}))

        @job = FactoryGirl.build :job
        @socket = double('Broker::Socket', :send_command => nil)
        subject.stub(:socket => @socket)
        @socket.should_receive(:send_command) do |cmd|
          #expect(cmd.worker_id).to eq(worker_id)
          expect(cmd.parameters[:command]).to eq('job')
          expect(cmd.parameters[:target]).to eq(@job.target)
        end

        job_class = double('JobClass')
        job_class.stub(:dequeue_job_for_queue).and_return(@job)
        JobDispatch.config.job_class = job_class

        # dispatch a job to a worker
        subject.dispatch_jobs_to_workers

        # now get status
        result = subject.process_command(command)

        expect(result.parameters[:queues]).to be_a(Hash)
        expect(result.parameters[:queues].size).to eq(1)
        expect(result.parameters[:queues][:default]).to be_a(Hash)
        expect(result.parameters[:queues][:default][worker_id2.to_hex]).to be_a(Hash)
        expect(result.parameters[:queues][:default][worker_id2.to_hex][:status]).to eq(:processing)
        expect(result.parameters[:queues][:default][worker_id2.to_hex][:job_id]).to eq(@job.id)
        expect(result.parameters[:queues][:default][worker_id2.to_hex][:name]).to eq('test worker 1')
        expect(result.parameters[:queues][:default][worker_id3.to_hex]).to be_a(Hash)
        expect(result.parameters[:queues][:default][worker_id3.to_hex][:status]).to eq(:idle)
        expect(result.parameters[:queues][:default][worker_id3.to_hex][:name]).to eq('test worker 2')
      end
    end

    context "'quit'" do
      let(:command) { Command.new(worker_id, {command: 'quit'}) }

      it "returns a command" do
        @result = subject.process_command(command)
        expect(@result).to be_a(Command)
      end

      it "acknowledges the command" do
        @result = subject.process_command(command)
        expect(@result.parameters[:status]).to eq("bye")
      end

      it "sets running to false" do
        @result = subject.process_command(command)
        expect(subject.running?).to be_false
      end

      it "sends a quit message to a waiting worker" do
        socket = double('Broker::Socket', :send_command => command)
        socket.should_receive(:send_command) do |cmd|
          expect(cmd.worker_id).to eq(worker_id)
        end
        subject.workers_waiting_for_reply << worker_id
        subject.process_command(Command.new(worker_id, command: 'ready'))
        subject.stub(:socket => socket)
        @result = subject.process_command(command)
      end
    end

    # when a worker is ready for work!
    context "'ready'" do
      let(:command) { Command.new(worker_id, {command: 'ready', queue: 'example', worker_name: 'ruby worker'}) }
      it "returns nil" do
        @result = subject.process_command(command)
        expect(@result).to be_nil
      end

      it "adds the worker to the list of workers awaiting replies" do
        @result = subject.process_command(command)
        expect(subject.workers_waiting_for_jobs.keys).to include(worker_id)
      end

      it "adds the worker to the queue" do
        @result = subject.process_command(command)
        expect(subject.queues[:example]).to include(worker_id)
      end

      it "adds the worker's name to the hash of worker names" do
        @result = subject.process_command(command)
        expect(subject.worker_names[worker_id]).to eq('ruby worker')
      end

      let(:command2) { Command.new(worker_id2, {command: 'ready', queue: 'example', worker_name: 'ruby worker'}) }

      it "duplicate ready has only 1 worker" do
        @result = subject.process_command(command)
        @result = subject.process_command(command2)
        expect(subject.queues[:example].count).to eq(1)
      end
    end


    context "'goodbye'" do
      # the goodbye command is sent from a new socket, so the actual socket identity will be different from
      # the socket waiting for a job or idle command.
      let(:worker_name) { 'ruby worker' }
      let(:goodbye_command) { Command.new(worker_id2, {command: 'goodbye', worker_name: worker_name}) }
      let(:ready_command) { Command.new(worker_id, {command: 'ready', queue: 'example', worker_name: worker_name}) }
      context 'with idle worker' do
        before :each do
          subject.process_command(ready_command)
        end

        it "returns an object" do
          @result = subject.process_command(goodbye_command)
          expect(@result).to be_a(Command)
          expect(@result.parameters).to be_a(Hash)
        end

        it "removes the worker's name from the worker_name hash" do
          @result = subject.process_command(goodbye_command)
          expect(subject.worker_names).not_to include(worker_id)
        end

        it "removes the worker from the list waiting reply" do
          subject.process_command(goodbye_command)
          expect(subject.workers_waiting_for_reply).not_to include(worker_id)
        end

        it "removes the worker from the list of workers ready for jobs" do
          subject.process_command(goodbye_command)
          expect(subject.workers_waiting_for_jobs).not_to include(worker_id)
        end
      end

      context 'without an idle worker' do
        it "returns an object" do
          @result = subject.process_command(goodbye_command)
          expect(@result).to be_a(Command)
          expect(@result.parameters).to be_a(Hash)
        end

        it "removes the worker's name from the worker_name hash" do
          @result = subject.process_command(goodbye_command)
          expect(subject.worker_names).not_to include(worker_id)
        end

        it "removes the worker from the list waiting reply" do
          subject.process_command(goodbye_command)
          expect(subject.workers_waiting_for_reply).not_to include(worker_id)
        end

        it "removes the worker from the list of workers ready for jobs" do
          subject.process_command(goodbye_command)
          expect(subject.workers_waiting_for_jobs).not_to include(worker_id)
        end
      end
    end

    # a worker has completed a task and is optionally asking for another one.
    context "'completed'" do
      let(:job_id) { '1234' }
      before :each do
        @job = FactoryGirl.build :job, :id => job_id
        @job.stub(:succeeded!) do
          @job.status = JobDispatch::Job::COMPLETED
          true
        end
        @job.stub(:failed!) do
          @job.status = JobDispatch::Job::FAILED
          true
        end
        @job.stub(:reload => true)
        subject.jobs_in_progress[job_id] = @job
        JobDispatch.config.job_class.stub(:find) do |id|
          raise StandardError, "Job not found" if id != @job.id
          @job
        end
      end

      context "and ask for another job" do
        let(:command) { Command.new(worker_id, {
            command: 'completed',
            job_id: job_id,
            status: 'success',
            result: 'the result',
            ready: true,
            queue: 'example'
        }) }

        it "returns nil" do
          @result = subject.process_command(command)
          expect(@result).to be_nil
        end

        it "adds the worker to the list of workers awaiting replies" do
          @result = subject.process_command(command)
          expect(subject.workers_waiting_for_jobs.keys).to include(worker_id)
        end

        it "adds the worker to the queue" do
          @result = subject.process_command(command)
          expect(subject.queues[:example]).to include(worker_id)
        end

        it "marks the job as succeeded" do
          @job.should_receive(:succeeded!).with('the result')
          subject.process_command(command)
        end
      end

      context 'and not asking for another job' do
        let(:command) { Command.new(worker_id, {
            command: 'completed',
            job_id: job_id,
            status: 'success',
            result: 'the result'
        }) }

        it "returns thanks" do
          @result = subject.process_command(command)
          expect(@result).to be_a(Command)
          expect(@result.parameters[:status]).to eq('thanks')
        end

        it "adds the worker to the list of workers awaiting replies" do
          @result = subject.process_command(command)
          expect(subject.workers_waiting_for_jobs.keys).not_to include(worker_id)
        end

        it "adds the worker to the queue" do
          @result = subject.process_command(command)
          expect(subject.queues[:example]).not_to include(worker_id)
        end

        it "marks the job as succeeded" do
          @job.should_receive(:succeeded!).with('the result')
          subject.process_command(command)
        end
      end

      context 'when the job fails, it is marked as failed' do
        let(:command) { Command.new(worker_id, {
            command: 'completed',
            job_id: job_id,
            status: 'error',
            result: 'the error message'
        }) }

        it "marks the job as failed" do
          @job.should_receive(:failed!).with('the error message')
          subject.process_command(command)
        end
      end

      context "when the job doesn't exist" do
        let(:command) { Command.new(worker_id, {
            command: 'completed',
            job_id: 'wrong',
            status: 'success',
            result: 'the result'
        }) }

        it "returns 'thanks' anyway" do
          JobDispatch.config.job_class.stub(:find).and_raise(StandardError, "Job not found")
          @result = subject.process_command(command)
          expect(@result).to be_a(Command)
          expect(@result.parameters[:status]).to eq('thanks')
        end
      end


      # This context is for when a job is being executed by a worker and the timeout has been reached.
      # The broker will have purged the job from the list of jobs in progress, and the worker will be
      # offline since it has not yet asked for more work to do. If the job completes successfully, the
      # broker needs to update the status of the job to completed (or failed) appropriately, which will
      # override any retry. In practice, long jobs should use the "touch" command to let the broker
      # know that they are in fact still working on the job and to please extend the timeout deadline.
      context "after the job has timed out" do
        let(:command) { Command.new(worker_id, {
            command: 'completed',
            job_id: @job.id,
            status: 'success',
            result: 'the result',
            ready: true,
            queue: 'example'
        }) }

        before :each do
          @job.stub(:as_json).and_return({status: @job.status, result: @job.result, id: @job.id})
          @job.status = 1 # mark as in progress
          JobDispatch.config.job_class.stub(:dequeue_job_for_queue).and_return(@job)

          # simulate the job has expired:
          @job.stub(:timed_out? => true)
          subject.expire_timed_out_jobs
        end

        it "Updates the job status to be completed" do
          subject.process_command(command)
          expect(@job.status).to eq(JobDispatch::Job::COMPLETED)
        end

        it "adds the worker to the queue" do
          subject.process_command(command)
          expect(subject.queues[:example]).to include(worker_id)
        end

        it "marks the job as succeeded" do
          @job.should_receive(:succeeded!).with('the result')
          subject.process_command(command)
        end

      end
    end

  end


  context "idle workers" do

    before :each do
      @socket = double('Broker::Socket', :send_command => nil)
      subject.stub(:socket => @socket)

      @time = Time.now
      # this worker will be IDLE
      Timecop.freeze(@time) do
        subject.workers_waiting_for_reply << worker_id # ugly: simulating a prior read_command (implementation detail!)
        command = Command.new(worker_id, {command: 'ready', queue: 'example'})
        @result = subject.process_command(command)
      end
      # this worker should stay in the queue
      Timecop.freeze(@time + 5) do
        subject.workers_waiting_for_reply << worker_id2 # ugly: simulating a prior read_command (implementation detail!)
        command = Command.new(worker_id2, {command: 'ready', queue: 'example'})
        @result = subject.process_command(command)
      end
    end

    context "already done an idle" do
      before do
        JobDispatch::Broker::IdleWorker.any_instance.stub(:idle_count => 1)
      end

      it "that have waited long enough receive idle commands" do
        @socket.should_receive(:send_command) do |cmd|
          expect(cmd.worker_id).to eq(worker_id)
          expect(cmd.parameters[:command]).to eq('idle')
        end

        Timecop.freeze(@time + JobDispatch::Broker::WORKER_IDLE_TIME + 1) do
          subject.send_idle_commands
        end

        expect(subject.workers_waiting_for_reply).not_to include(worker_id)
        expect(subject.queues[:example]).not_to include(worker_id)
      end

      it "that have not waited long enough are still waiting" do
        @socket.should_receive(:send_command) do |cmd|
          expect(cmd.worker_id).not_to eq(worker_id2)
        end

        Timecop.freeze(@time + JobDispatch::Broker::WORKER_IDLE_TIME + 1) do
          subject.send_idle_commands
        end

        expect(subject.workers_waiting_for_reply).to include(worker_id2)
        expect(subject.queues[:example]).to include(worker_id2)
      end
    end

    context "have not done an idle" do
      before do
        JobDispatch::Broker::IdleWorker.any_instance.stub(:idle_count => 0)
      end

      it "that have waited long enough receive idle commands" do
        @socket.should_receive(:send_command) do |cmd|
          expect(cmd.worker_id).to eq(worker_id)
          expect(cmd.parameters[:command]).to eq('idle')
        end

        Timecop.freeze(@time + JobDispatch::Broker::WORKER_IDLE_TIME + 1) do
          subject.send_idle_commands
        end

        expect(subject.workers_waiting_for_reply).not_to include(worker_id)
        expect(subject.queues[:example]).not_to include(worker_id)
      end

      it "that have not waited long enough are still waiting" do
        @socket.should_receive(:send_command) do |cmd|
          expect(cmd.worker_id).not_to eq(worker_id2)
        end

        Timecop.freeze(@time + JobDispatch::Broker::WORKER_IDLE_TIME + 1) do
          subject.send_idle_commands
        end

        expect(subject.workers_waiting_for_reply).not_to include(worker_id2)
        expect(subject.queues[:example]).not_to include(worker_id2)
      end
    end
  end


  context "dispatching jobs" do
    context "when there are jobs in a queue" do
      before :each do
        @job = FactoryGirl.build :job
        @socket = double('Broker::Socket', :send_command => nil)
        subject.stub(:socket => @socket)
        @job_class = double('JobClass')
        JobDispatch.config.job_class = @job_class
      end

      it "the job is sent to an idle worker" do
        @socket.should_receive(:send_command) do |cmd|
          expect(cmd.worker_id).to eq(worker_id)
          expect(cmd.parameters[:command]).to eq('job')
          expect(cmd.parameters[:target]).to eq(@job.target)
        end

        @job_class.stub(:dequeue_job_for_queue).and_return(@job)
        @job_class.should_receive(:dequeue_job_for_queue).with('example')

        # send ready command => adds idle worker state
        subject.workers_waiting_for_reply << worker_id # simulating read_command
        @result = subject.process_command(Command.new(worker_id, {
            command: 'ready',
            queue: 'example',
            worker_name: 'ruby worker',
        }))
        expect(@result).to be_nil # no immediate response
        expect(subject.workers_waiting_for_jobs[worker_id]).not_to be_nil
        subject.queues_ready[:example] = true

        subject.dispatch_jobs_to_workers
      end

      it "when no job is found, the queue is marked inactive" do
        # send ready command => adds idle worker state
        subject.workers_waiting_for_reply << worker_id # simulating read_command
        @result = subject.process_command(Command.new(worker_id, {
            command: 'ready',
            queue: 'example',
            worker_name: 'ruby worker',
        }))

        @job_class.stub(:dequeue_job_for_queue).and_return(nil)

        expect(@result).to be_nil # no immediate response
        expect(subject.workers_waiting_for_jobs[worker_id]).not_to be_nil
        subject.queues_ready[:example] = true

        subject.dispatch_jobs_to_workers
        expect(subject.queues_ready[:example]).to be_false
      end
    end

    context "when an error occurs dequeuing jobs" do
      before :each do
        @job_class = double('JobClass')
        @job_class.stub(:dequeue_job_for_queue).and_raise(StandardError, "something bad happened")
        JobDispatch.config.job_class = @job_class
      end

      it "behaves as if there was no job" do
        # send ready command => adds idle worker state
        subject.workers_waiting_for_reply << worker_id # simulating read_command
        @result = subject.process_command(Command.new(worker_id, {
            command: 'ready',
            queue: 'example',
            worker_name: 'ruby worker',
        }))
        expect(@result).to be_nil # no immediate response
        expect(subject.workers_waiting_for_jobs[worker_id]).not_to be_nil

        # no job should be sent
        subject.should_not_receive(:send_job_to_worker)
        expect { subject.dispatch_jobs_to_workers }.not_to raise_error
      end
    end
  end

  context "expired jobs" do
    it "are removed from jobs list when they expire" do
      time = Time.now
      @job = FactoryGirl.build :job, :expire_execution_at => time - 5.seconds
      @job_id = @job.id.to_s
      subject.jobs_in_progress[@job_id] = @job
      subject.jobs_in_progress_workers[@job_id] = worker_id
      subject.expire_timed_out_jobs
      expect(subject.jobs_in_progress).to be_empty
      expect(subject.jobs_in_progress_workers).to be_empty
    end


    it "include InternalJob commands" do
      socket = double('Broker::Socket')
      subject.stub(:socket => socket)
      socket.stub(:send_command => true)

      # send ready command => adds idle worker state
      subject.workers_waiting_for_reply << worker_id # simulating read_command
      @result = subject.process_command(Command.new(worker_id, {
          command: 'ready',
          queue: 'example',
          worker_name: 'ruby worker',
      }))
      subject.send_idle_commands(Time.now + JobDispatch::Broker::WORKER_IDLE_TIME + 10)
      expect(subject.jobs_in_progress_workers.length).to eq(1)
      @time = Time.now + JobDispatch::Broker::WORKER_IDLE_TIME + 10 + JobDispatch::Job::DEFAULT_EXECUTION_TIMEOUT + 10
      JobDispatch::Broker::InternalJob.any_instance.should_not_receive(:failed!)
      Timecop.freeze(@time) do
        subject.expire_timed_out_jobs
      end
    end
  end

  context "touching a job" do
    before :each do
      @time = Time.now.change(:usec => 0)
      # this worker will be IDLE
      @job = FactoryGirl.build :job, :expire_execution_at => @time + 5.seconds
      @job_id = @job.id.to_s
      subject.jobs_in_progress[@job_id] = @job
      subject.jobs_in_progress_workers[@job_id] = worker_id
      @socket = double('Broker::Socket', :send_command => nil)
      subject.stub(:socket => @socket)
      @socket.stub(:read_command).and_return(nil)
      @job.stub(:save)
    end

    it "updates the expires_execute_at time" do
      Timecop.freeze(@time) do
        subject.touch_job(Command.new(worker_id, {command: "touch", job_id: @job_id}))
      end
      expect(@job.expire_execution_at).to eq(@time + @job.timeout)
    end

    it "updates the expire_execution_at time with a custom timeout" do
      Timecop.freeze(@time) do
        subject.touch_job(Command.new(worker_id, {command: "touch", job_id: @job_id, timeout: 100}))
      end
      expect(@job.expire_execution_at).to eq(@time + 100.seconds)
    end
  end

  context "enqueue a job" do
    before :each do
      @job_attrs = FactoryGirl.attributes_for :job
      JobDispatch.config.job_class = double('JobClass')
    end

    it "Creates a job" do
      JobDispatch.config.job_class.should_receive(:create!).with(@job_attrs)
      command = Command.new(:some_client, {command: "enqueue", job: @job_attrs})
      subject.process_command(command)
    end

    it "returns the job id" do
      job_id = 12345
      JobDispatch.config.job_class.stub(:create! => double('Job', :id => job_id))
      command = Command.new(:some_client, {command: "enqueue", job: @job_attrs})
      result = subject.process_command(command)
      expect(result.parameters[:job_id]).to eq(job_id.to_s)
      job_id
    end

    it "returns an error if the arguments are no good" do
      JobDispatch.config.job_class.stub(:create!).and_raise("no good") # simulate some database save error
      command = Command.new(:some_client, {command: "enqueue", job: @job_attrs})
      result = subject.process_command(command)
      expect(result.parameters[:status]).to eq('error')
      expect(result.parameters[:message]).to eq('no good')
    end

    it "returns an error if the 'job' parameter is missing" do
      command = Command.new(:some_client, {command: "enqueue"})
      result = subject.process_command(command)
      expect(result.parameters[:status]).to eq('error')
    end

    it "marks the queue as ready" do
      JobDispatch.config.job_class.stub(:create! => double('Job', :id => 1))
      command = Command.new(:some_client, {command: "enqueue", job: @job_attrs})
      result = subject.process_command(command)
      expect(subject.queues_ready[:default]).to be_true
    end
  end

  context "'notify'" do

    before :each do
      @job = FactoryGirl.build :job
      @job_class = double('JobClass')
      @job_class.stub(:dequeue_job_for_queue).and_return(@job)
      @job_class.stub(:find) do |id|
        @job if id == @job.id
      end
      JobDispatch.config.job_class = @job_class
    end

    context "with no jobs" do
      it "returns no such job" do
        command = Command.new(:client, {command: 'notify', job_id: 1234})
        @job_class.should_receive(:find).with(1234).and_raise(StandardError, "bozo")
        result = subject.process_command(command)
        expect(result.parameters[:status]).to eq('error')
        expect(result.parameters[:message]).to eq('bozo')
      end
    end

    context "with a completed job" do
      it "returns the job result" do
        @job = FactoryGirl.build :job, status: JobDispatch::Job::COMPLETED, result: 'hooray'
        command = Command.new(:client, {command: 'notify', job_id: @job.id})
        @job_class.should_receive(:find).with(@job.id).and_return(@job)
        result = subject.process_command(command)
        expect(result.parameters[:status]).to eq('completed')
      end
    end

    context "with a job in progress" do
      before :each do
        @socket = double('Broker::Socket')
        subject.stub(:socket => @socket)
        @socket.stub(:send_command => nil)

        # worker ready command

        subject.workers_waiting_for_reply << worker_id # simulating read_command
        @result = subject.process_command(Command.new(worker_id, {
            command: 'ready',
            queue: 'example',
            worker_name: 'ruby worker',
        }))
        expect(@result).to be_nil # no immediate response
        expect(subject.workers_waiting_for_jobs[worker_id]).not_to be_nil

        # dispatch job to worker

        subject.dispatch_jobs_to_workers

        # send notify command

        subject.workers_waiting_for_reply << worker_id2 # simulating read_command
        @result = subject.process_command(Command.new(worker_id2, {
            command: 'notify',
            job_id: @job.id
        }))
      end

      it "registers the job subscriber" do
        expect(@result).to be_nil # no immediate response
        expect(subject.job_subscribers[@job.id]).to include(worker_id2)
        expect(subject.workers_waiting_for_reply).to include(worker_id2)
      end

      it "returns when the job completes" do
        # when the worker completes, the notify socket should be notified that
        # the job completed.
        socket2 = double('Broker::Socket')
        subject.stub(:socket => socket2)
        socket2.should_receive(:send_command) do |cmd|
          expect(cmd.worker_id).to eq(worker_id2)
          expect(cmd.parameters[:status]).to eq('completed')
          expect(cmd.parameters[:job_id]).to eq(@job.id)
        end

        expect(subject.workers_waiting_for_reply).to include(worker_id2)

        # worker completed job -> should send response to notify socket.
        subject.workers_waiting_for_reply << worker_id # simulating read_command
        @result = subject.process_command(Command.new(worker_id, {
            command: 'completed',
            job_id: @job.id,
            status: 'success',
            result: 'foobar'
        }))
      end

      it "returns when a job fails" do
        # when the worker completes, the notify socket should be notified that
        # the job completed.
        socket2 = double('Broker::Socket')
        subject.stub(:socket => socket2)
        socket2.should_receive(:send_command) do |cmd|
          expect(cmd.worker_id).to eq(worker_id2)
          expect(cmd.parameters[:status]).to eq('failed')
          expect(cmd.parameters[:job_id]).to eq(@job.id)
        end

        expect(subject.workers_waiting_for_reply).to include(worker_id2)

        # worker completed job -> should send response to notify socket.
        subject.workers_waiting_for_reply << worker_id # simulating read_command
        @result = subject.process_command(Command.new(worker_id, {
            command: 'completed',
            job_id: @job.id,
            status: 'error',
            result: 'foobar'
        }))
      end


      it "returns when a job times out" do
        # when the worker completes, the notify socket should be notified that
        # the job completed.
        socket2 = double('Broker::Socket')
        subject.stub(:socket => socket2)
        socket2.should_receive(:send_command) do |cmd|
          expect(cmd.worker_id).to eq(worker_id2)
          expect(cmd.parameters[:status]).to eq('failed')
          expect(cmd.parameters[:job_id]).to eq(@job.id)
        end

        expect(subject.workers_waiting_for_reply).to include(worker_id2)

        subject.jobs_in_progress[@job.id].stub(:timed_out? => true)
        subject.expire_timed_out_jobs
      end
    end
  end

  context "last" do
    let(:json){ {'id' => '12341234', 'target' => 'Example', 'method' => 'method'}}
    before do
      @job_class = double('JobClass')
      JobDispatch.config.job_class = @job_class
    end
    it "returns last job in specified queue" do
      command = Command.new(:client, {command: 'last', queue: 'ruby'})
      relation = double('relation')
      @job_class.should_receive(:where).with(queue: 'ruby').and_return(relation)
      relation.should_receive(:last).and_return(double("job", id: "12341234", as_json: json))
      result = subject.process_command(command)
      expect(result.parameters).to eq({status: 'success', job: json})
    end
    it "returns last job in default queue" do
      command = Command.new(:client, {command: 'last'})
      relation = double('relation')
      @job_class.should_receive(:where).with(queue: 'default').and_return(relation)
      relation.should_receive(:last).and_return(double("job", id: "12341234", as_json: json))
      result = subject.process_command(command)
      expect(result.parameters).to eq({status: 'success', job: json})
    end
    it "handles no last job" do
      command = Command.new(:client, {command: 'last'})
      relation = double('relation')
      @job_class.should_receive(:where).with(queue: 'default').and_return(relation)
      relation.should_receive(:last).and_return(nil)
      result = subject.process_command(command)
      expect(result.parameters).to eq({status: 'error', message: 'no last job'})
    end
  end

  context "fetch" do
    before do
      @job_class = double('JobClass')
      JobDispatch.config.job_class = @job_class
    end
    it "returns the job" do
      command = Command.new(:client, {command: 'fetch', job_id: '12341234'})
      json = {'id' => '12341234', 'queue' => 'ruby', 'target' => 'String', 'method' => 'new'}
      job = double("Job", as_json: json)
      @job_class.should_receive(:find).with('12341234').and_return(job)
      result = subject.process_command(command)
      expect(result.parameters).to eq({status: 'success', job: json})
    end
    it "returns error when job_id is not present" do
      command = Command.new(:client, {command: 'fetch'})
      result = subject.process_command(command)
      expect(result.parameters[:status]).to eq('error')
    end
    it "returns error when job_id is not found" do
      command = Command.new(:client, {command: 'fetch', job_id: '12341234'})
      @job_class.should_receive(:find).with('12341234').and_raise(StandardError, "not found")
      result = subject.process_command(command)
      expect(result.parameters[:status]).to eq('error')
    end
  end
end
