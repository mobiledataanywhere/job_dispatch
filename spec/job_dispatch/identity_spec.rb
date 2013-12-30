require 'rspec'
require 'spec_helper'

describe JobDispatch::Identity do

  context "with a string" do

    subject { JobDispatch::Identity.new('hello')}

    it "returns a string" do
      expect(subject.to_s).to eq('hello')
    end
    it "returns a symbol" do
      expect(subject.to_sym).to eq(:hello)
    end
    it "returns hex string" do
      expect(subject.to_hex).to eq('68656c6c6f')
    end
  end

  context "with a symbol" do

    subject { JobDispatch::Identity.new(:hello)}

    it "returns a string" do
      expect(subject.to_s).to eq('hello')
    end
    it "returns a symbol" do
      expect(subject.to_sym).to eq(:hello)
    end
    it "returns hex string" do
      expect(subject.to_hex).to eq('68656c6c6f')
    end
  end


  context "with no-printable characters" do
    let(:worker_id) { [0, 0x80, 0, 0x41, 0xB9].pack('c*') }
    subject { JobDispatch::Identity.new(worker_id)}

    it "returns a string" do
      expect(subject.to_s).to eq(worker_id)
    end
    it "returns a symbol" do
      expect(subject.to_sym).to eq(worker_id.to_sym)
    end
    it "returns hex string" do
      expect(subject.to_hex).to eq('00800041b9')
    end
  end

  context "testing equality" do

    context "with same identity" do
      before :each do
        @a = JobDispatch::Identity.new('a')
        @b = JobDispatch::Identity.new('a')
      end

      it "is equal to another with the same identity" do
        expect(@a == @b).to be_true
      end

      it "can look up an item from a hash" do
        hash = {}
        hash[@a] = 'hello'
        expect(hash[@b]).to eq('hello')
      end
    end

    context "with different identity" do
      before :each do
        @a = JobDispatch::Identity.new('a')
        @b = JobDispatch::Identity.new('b')
      end

      it "is not equal to another with the same identity" do
        expect(@a == @b).to be_false
      end

      it "can look up an item from a hash" do
        hash = {}
        hash[@a] = 'hello'
        expect(hash[@b]).to be_nil
      end
    end
  end
end
