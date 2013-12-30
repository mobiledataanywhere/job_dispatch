# JobDispatch compliant job.
#
class Job

  include Mongoid::Document

  # important: Need this to include the success! and fail! methods.
  include JobDispatch::Job

  # required fields:
  field :queue, :type => String, :default => 'default'
  field :status, :type => Integer, :default => PENDING

  field :parameters, :type => Array # list of parameters for the action
  field :target, :type => String
  field :method, :type => String

  field :enqueued_at, :type => Time #, :default => -> { Time.now.to_i }
  field :scheduled_at, :type => Time, :default => 0
  field :expire_execution_at, :type => Time
  field :timeout, :type => Integer
  field :retry_count, :type => Integer, :default => 0
  field :retry_delay, :type => Integer, :default => 10
  field :completed_at, :type => Time

  # If the job completed successfully, this is the result. If it failed,
  # this is the exception error message
  field :result, :type => Object

  # any indexes:
  index({queue: 1, status: 1, scheduled_at: 1})

  # dequeue a job from the database.
  # This is an atomic operation that also marks the job as being in the pending state.
  def self.dequeue_job_for_queue(queue, time=nil)
    time ||= Time.now
    self.
        where(:queue => queue, :status => PENDING).
        where(:scheduled_at.lte => time).
        sort(:enqueued_at => 1).
        find_and_modify({"$set" => {:status => IN_PROGRESS}}, new: true)
  end
end
