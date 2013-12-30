require 'spec_helper'

describe JobDispatch::Broker::Socket do

  subject { JobDispatch::Broker::Socket.new('tcp://localhost:1999') }

  context "Reading messages from a worker" do
    before :each do
      @socket = double('Socket')
      subject.stub(:socket => @socket)
    end

    context "with a valid message" do
      before :each do
        message = ZMQ::Message.new
        message.addstr(JSON.dump({command: 'ready', queue: 'my_queue'}))
        message.wrap(ZMQ::Frame('my_worker_id'))
        @socket.stub(:recv_message => message)
        @command = subject.read_command
      end

      it "returns a Command" do
        expect(@command).to be_a(JobDispatch::Broker::Command)
      end

      it "reads the command" do
        expect(@command.parameters[:command]).to eq('ready')
      end

      it "reads the worker id" do
        expect(@command.worker_id.to_sym).to eq(:my_worker_id)
      end
    end

    context "with an invalid message" do
      before :each do
        message = ZMQ::Message.new
        message.addstr("Hello")
        message.wrap(ZMQ::Frame('my_worker_id'))
        @socket.stub(:recv_message => message)
        @command = subject.read_command
      end

      it "returns a Command" do
        expect(@command).to be_a(JobDispatch::Broker::Command)
      end

      it "reads the worker id" do
        expect(@command.worker_id.to_sym).to eq(:my_worker_id)
      end
    end
  end
end
