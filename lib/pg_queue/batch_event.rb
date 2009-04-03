module PGQueue
  class BatchEvent
    attr_reader :connection, :batch_id

    def self.event_type(type)
      PGQueue::BatchEvent.registered_event_types[type.to_s] = self.class
    end
    
    def self.alias_ev_fields(map)
      map.each do |method_name, field_name|
        alias_method method_name.to_sym, field_name.to_sym
      end
    end
    
    def self.new(connection_, batch_id_, db_row)
      if self == PGQueue::BatchEvent
        @type_idx ||= columns.index(:ev_type)
        registered_class = registered_event_types[db_row[@type_idx]]
        return registered_class.new(connection_, batch_id_, db_row) if registered_class
      end
      
      super
    end
    
    def initialize(connection_, batch_id_, db_row)
      @connection = connection_
      @batch_id   = batch_id_
      
      self.class.columns.each_with_index do |col, ii|
        instance_variable_set("@#{col}".to_sym, db_row[ii])
      end
    end
    
    def fail!(reason)
      connection.query("SELECT pgq.event_failed(#{batch_id}, #{ev_id}, '#{connection.quote_string(reason)}')")
    end
    
    def retry!(retry_seconds)
      connection.query("SELECT pgq.event_retry(#{batch_id}, #{ev_id}, #{retry_seconds})")
    end
    
  protected
    def self.registered_event_types
      @registered_ev_type ||= {}
    end
    
  private
    def self.columns
      [:ev_id, :ev_time, :ev_txid, :ev_retry, :ev_type, :ev_data, :ev_extra1, :ev_extra2, :ev_extra3, :ev_extra4]
    end

  public
    columns.each {|col| attr_reader col }
  end
end