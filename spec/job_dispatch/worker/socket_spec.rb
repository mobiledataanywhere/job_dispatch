require 'rspec'
require 'spec_helper'

describe JobDispatch::Worker::Socket do

  let(:endpoint) { 'ipc://test' }
  let(:item_class) do
    double('ItemClass')
  end
  subject { JobDispatch::Worker::Socket.new(endpoint, item_class) }

  context "receiving a message" do
    before :each do
      @item = double('Item')
      @item.stub(:job_id=)
      item_class.stub(:new).and_return(@item)
    end

    it "creates an item of the right class" do
      item_class.should_receive(:new)
      subject.socket.stub(:recv).and_return(JSON.dump({command:'idle'}))
      expect(subject.read_item).to eq(@item)
    end

    it "quits when a quit message is received" do
      Process.stub(:exit).and_return(nil)
      Process.should_receive(:exit).with(0)
      subject.socket.stub(:recv).and_return(JSON.dump({command:'quit'}))
      subject.read_item
    end
  end
end
