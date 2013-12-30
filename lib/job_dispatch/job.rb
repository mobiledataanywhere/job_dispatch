module JobDispatch
  module Job

    DEFAULT_EXECUTION_TIMEOUT = 30

    PENDING = 0
    IN_PROGRESS = 1
    COMPLETED = 2
    FAILED = 3

    STATUS_STRINGS = {
        PENDING => 'pending',
        IN_PROGRESS => 'in progress',
        COMPLETED => 'completed',
        FAILED => 'failed'
    }

    def timed_out?
      expire_execution_at < Time.now
    end

    def failed!(results)
      # update database
      self.completed_at = Time.now
      self.result = results
      if retry_count && retry_count > 0 && retry_delay && retry_delay > 0
        self.retry_count -= 1
        self.scheduled_at = Time.now + retry_delay.seconds
        self.retry_delay *= 2
        self.status = PENDING
      else
        self.status = FAILED
      end
      save!
    end

    def succeeded!(results)
      self.status = COMPLETED
      self.result = results
      self.completed_at = Time.now
      save!
    end
  end
end
