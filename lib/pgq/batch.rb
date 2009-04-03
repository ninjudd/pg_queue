module PGQ
  class Batch
    attr_reader :connection, :queue_name, :consumer_id, :batch_id
    
    def initialize(connection_, queue_name_, consumer_id_)
      @connection  = connection_
      @queue_name  = queue_name_
      @consumer_id = consumer_id_
    end

    def next_event
      get_batch
      BatchEvent.new(connection, batch_id, next_event_row)
    end
    
    def each_event(&block)
      while (batch_event = next_event) != nil
        yield batch_event
      end
    end
    
  private
    def self.columns
      [:ev_id, :ev_time, :ev_txid, :ev_retry, :ev_type, :ev_data, :ev_extra1, :ev_extra2, :ev_extra3, :ev_extra4]
    end
    
    def get_batch
      return unless @batch_id.nil?
      @batch_id = connection.select_value("SELECT pgq.next_batch('#{queue_name}', '#{consumer_id}')")
      @event_rows = connection.exec("SELECT pgq.get_batch_events(#{batch_id})")
    end
    
    def next_event_row
      row = @event_rows ? @event_rows.shift : nil
      finish_batch if row.nil?
      row
    end
    
    def finish_batch
      connection.exec("SELECT pgq.finish_batch(#{@batch_id})") if @batch_id
      @batch_id = nil
    end
  end
end
