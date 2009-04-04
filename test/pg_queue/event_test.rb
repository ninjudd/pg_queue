require File.dirname(__FILE__) + '/../test_helper'

class PGQueueEventTest < Test::Unit::TestCase
  context "A User instance" do
    setup do
      row = ['1', '2009-04-01 17:55:42', '2', nil, 'generic_type', 'some_data1', 'ex1', 'ex2', 'ex3', 'ex4']
      
      @connection = mock
      @event = PGQueue::Event.new(row, :connection => @connection, :batch_id => 17)
    end
    
    should "map columns to attributes" do
      assert_equal 1,                             @event.id
      assert_equal Time.local(2009,4,1,17,55,42), @event.time
      assert_equal 'generic_type',                @event.type
      assert_equal 'some_data1',                  @event.data
      assert_equal 'ex1',                         @event.extra1
      assert_equal 'ex2',                         @event.extra2
      assert_equal 'ex3',                         @event.extra3
      assert_equal 'ex4',                         @event.extra4
    end
    
    should "call pgq.event_failed" do
      @connection.expects(:quote).with("can't handle the truth").returns("'can''t handle the truth'")
      @connection.expects(:exec).with("SELECT pgq.event_failed(17, 1, 'can''t handle the truth')")
      @event.fail!("can't handle the truth")
    end
    
    should "call pgq.event_retry" do
      @connection.expects(:exec).with("SELECT pgq.event_retry(17, 1, 360)")
      @event.retry!(360)
    end
  end
end
