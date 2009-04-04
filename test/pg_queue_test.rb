require File.dirname(__FILE__) + '/test_helper'

class PGQueueTest < Test::Unit::TestCase
  context "Iterating over a batch" do
    setup do
      @connection = mock
      @mock_batch_results = [ ['1', '2009-04-01', '2', nil, 'generic_type', 'some_data1', nil, nil, nil, nil],
                              ['2', '2009-04-01', '2', nil, 'generic_type', 'some_data2', nil, nil, nil, nil] ]
      
      @connection.expects(:quote).with('foo').returns("'foo'")
      @connection.expects(:quote).with('bar').returns("'bar'")
      @connection.expects(:exec).with("SELECT pgq.register_consumer('foo', 'bar')")
      @connection.expects(:query).with("SELECT pgq.next_batch('foo', 'bar')").returns(['17'])
      @connection.expects(:exec).with("SELECT pgq.get_batch_events(17)").returns(@mock_batch_results)
      @connection.expects(:exec).with("SELECT pgq.finish_batch(17)")
    end
    
    should "yield a series of batch_events" do      
      result_events = []
      q = PGQueue.new(:connection => @connection, :name => 'foo', :consumer_id => 'bar')
      q.each do |event|
        result_events << event
      end
      
      assert_equal 2,     result_events.size
      assert_equal [1,2], result_events.collect {|e| e.id }
    end
    
    should 'be enumerable' do
      q = PGQueue.new(:connection => @connection, :name => 'foo', :consumer_id => 'bar')
      result_events = q.to_a
      
      assert_equal 2,     result_events.size
      assert_equal [1,2], result_events.collect {|e| e.id }
    end
  end
end
