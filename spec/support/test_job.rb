# This class is a stand in for an ActiveRecord or Mongoid::Document style class
TestJob = Struct.new :id,
                     :queue,
                     :status,
                     :enqueued_at,
                     :expire_execution_at,
                     :scheduled_at,
                     :completed_at,
                     :result,
                     :target,
                     :method,
                     :parameters,
                     :retry_count,
                     :retry_delay,
                     :timeout do
  include JobDispatch::Job

  def save!
    true
  end

  def save
    true
  end

  def reload
    self
  end

end
