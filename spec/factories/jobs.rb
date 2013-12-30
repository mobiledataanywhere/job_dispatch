FactoryGirl.define do
  factory :job do
    id { SecureRandom.uuid }
    queue :default
    status JobDispatch::Job::PENDING
    parameters []
    target "SecureRandom"
    add_attribute(:method) { "uuid" }

    enqueued_at { Time.now }
    scheduled_at { Time.at(0) }
    expire_execution_at { nil}
    timeout 10
    retry_count 0
    retry_delay 20
    completed_at nil
    result nil
  end
end
