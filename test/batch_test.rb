require File.dirname(__FILE__) + '/test_helper'

class BatchTest < Test::Unit::TestCase
  should "yield a series of batch_events" do
    mock_batch_results = [ ['1', '2009', '2', nil, 'generic_type', 'some_data1', nil, nil, nil, nil],
                           ['2', '2009', '2', nil, 'generic_type', 'some_data2', nil, nil, nil, nil] ]
    conn_mock = mock
    conn_mock.expects(:select_value).with("SELECT pgq.next_batch('my_named_queue', 'my_consumer_id')").returns(17)
    conn_mock.expects(:exec).with("SELECT pgq.get_batch_events(17)").returns(mock_batch_results)
    conn_mock.expects(:exec).with("SELECT pgq.finish_batch(17)")
    
    result_events = []
    PGQueue::Batch.new(conn_mock, 'my_named_queue', 'my_consumer_id').each_event do |event|
      result_events << event
    end
    
    assert_equal 2,         result_events.size
    assert_equal ['1','2'], result_events.collect {|re| re.ev_id }
  end
end
