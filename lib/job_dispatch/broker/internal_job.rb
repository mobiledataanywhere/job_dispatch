module JobDispatch
  class Broker

    # a class to represent a worker processing a broker command that is not a job. It is still tracked by
    # the broker as if it was a job, though. If the worker does not reply, they will be timed out. And
    # status report will show the worker state as executing this command.
    class InternalJob
      attr :id, :command, :queue

      def initialize(command, queue)
        @id = SecureRandom.uuid
        @command = command
        @timeout_at = Time.now + JobDispatch::Job::DEFAULT_EXECUTION_TIMEOUT
        @queue = queue
      end

      def timed_out?
        Time.now > @timeout_at
      end

      def as_json
        {
            command: command,
            id: id,
            queue: queue,
            target: "JobDispatch",
            method: command
        }
      end
    end
  end
end
