module PGQueue
  class Batch
    attr_reader :connection, :queue_name, :consumer_id, :batch_id
    
    def initialize(connection_, queue_name_, consumer_id_)
      @connection  = connection_
      @queue_name  = queue_name_
      @consumer_id = consumer_id_
    end

    def each_event(&block)
      @batch_id = connection.select_value("SELECT pgq.next_batch('#{queue_name}', '#{consumer_id}')")
      result = connection.exec("SELECT pgq.get_batch_events(#{batch_id})")
      
      result.each do |row|
        yield BatchEvent.new(connection, batch_id, row)
      end
      connection.exec("SELECT pgq.finish_batch(#{batch_id})")
    end
    
  private
    def self.columns
      [:ev_id, :ev_time, :ev_txid, :ev_retry, :ev_type, :ev_data, :ev_extra1, :ev_extra2, :ev_extra3, :ev_extra4]
    end
  end
end
