require File.dirname(__FILE__) + '/../test_helper'

class PGQueueTriggerEventTest < Test::Unit::TestCase
  context "A User instance" do
    setup do
      row = ['1', '2009-04-01 17:55:42', '2', nil, 'trigger', 'first_names', 'UPDATE', '(1,Bob)', '(1,Robert)']
      
      @connection = mock
      
      column_sql = "SELECT column_name from information_schema.columns WHERE table_name = 'first_names'"
      columns = ['id','first_name']
      @connection.expects(:select_values).with(column_sql).returns(columns).at_least_once

      @event = PGQueue::Event.new(row, :connection => @connection, :batch_id => 17)
    end
    
    should "instantiate correct class" do
      assert_equal PGQueue::TriggerEvent, @event.class
    end
    
    should "map columns to trigger attributes" do
      assert_equal 'first_names',               @event.table_name
      assert_equal 'UPDATE',                    @event.operation
      assert_equal({'id' => '1',
                    'first_name' => 'Bob' },    @event.old_record)
      assert_equal({'id' => '1',
                    'first_name' => 'Robert' }, @event.new_record)
    end
  end
end
