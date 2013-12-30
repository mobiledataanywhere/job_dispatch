require 'rspec'
require 'spec_helper'

describe JobDispatch::Job do

  context "succeeded" do
    subject { TestJob.new(1, 'default', JobDispatch::Job::PENDING) }
    before(:each) { subject.succeeded!('result') }
    it "sets the status to COMPLETED" do
      expect(subject.status).to eq(JobDispatch::Job::COMPLETED)
    end

    it "sets the result" do
      expect(subject.result).to eq('result')
    end
    it "sets completed at" do
      expect(subject.completed_at).to be_within(1.second).of(Time.now)
    end
  end

  context "failed" do
    subject { TestJob.new(1, 'default', JobDispatch::Job::PENDING) }
    before(:each) { subject.failed!('error message') }
    it "sets the status to FAILED" do
      expect(subject.status).to eq(JobDispatch::Job::FAILED)
    end

    it "sets the result" do
      expect(subject.result).to eq('error message')
    end

    it "sets completed at" do
      expect(subject.completed_at).to be_within(1.second).of(Time.now)
    end

    context "and retryable" do
      before(:each) do
        subject.retry_count = 1
        subject.retry_delay = 10
        subject.failed!('error message')
      end
      it "sets the status to PENDING" do
        expect(subject.status).to eq(JobDispatch::Job::PENDING)
      end
      it "sets completed at" do
        expect(subject.completed_at).to be_within(1.second).of(Time.now)
      end
      it "sets the result" do
        expect(subject.result).to eq('error message')
      end
      it "decrements retry_count" do
        expect(subject.retry_count).to eq(0)
      end
      it "doubles the retry_delay" do
        expect(subject.retry_delay).to eq(20)
      end
    end

    context "and out of retries" do
      before(:each) do
        subject.retry_count = 0
        subject.retry_delay = 20
        subject.failed!('error message')
      end
      it "sets the status to PENDING" do
        expect(subject.status).to eq(JobDispatch::Job::FAILED)
      end
      it "sets completed at" do
        expect(subject.completed_at).to be_within(1.second).of(Time.now)
      end
      it "sets the result" do
        expect(subject.result).to eq('error message')
      end
    end
  end

end
