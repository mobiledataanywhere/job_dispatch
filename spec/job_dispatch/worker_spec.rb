require 'rspec'
require 'spec_helper'

describe JobDispatch::Worker do

  Item ||= JobDispatch::Worker::Item
  Socket ||= JobDispatch::Worker::Socket

  context "with custom item class" do
    before :each do
      @klass = double('ItemClass')
      @worker = JobDispatch::Worker.new('ipc://test', item_class: @klass)
    end
    it "initialises the custom item class" do
      expect(@worker.item_class).to eq(@klass)
    end

    it "sets the custom class on the socket" do
      @worker.connect
      expect(@worker.socket.item_class).to eq(@klass)
    end
  end

end
