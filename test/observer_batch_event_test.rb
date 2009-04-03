require File.dirname(__FILE__) + '/test_helper'

class ObserverBatchEventTest < Test::Unit::TestCase
  context "A User instance" do
    setup do
      row = ['1', '2009-04-01 17:55:42', '2', nil, 'trigger_observer', 'first_names', 'UPDATE', '1,Bob', '1,Robert']
      
      @connection = mock
      
      column_sql = "SELECT column_name from information_schema.columns WHERE table_name = 'first_names'"
      columns = ['id','first_name']
      @connection.expects(:select_values).with(column_sql).returns(columns).at_least_once

      @batch_event = PGQ::BatchEvent.new(@connection, 17, row)
    end
    
    should "instantiate correct class" do
      assert @batch_event.kind_of?(PGQ::ObserverBatchEvent)
    end
    
    should "map columns to trigger attributes" do
      assert_equal 'first_names',               @batch_event.table_name
      assert_equal 'UPDATE',                    @batch_event.operation
      assert_equal({'id' => '1',
                    'first_name' => 'Bob' },    @batch_event.old_row)
      assert_equal({'id' => '1',
                    'first_name' => 'Robert' }, @batch_event.new_row)
    end
  end
end
