require 'time'
require 'parsedate'

module PGQueue
  class Event
    attr_reader :connection, :batch_id

    def self.event_type(type)
      event_class_by_type[type.to_s] = self
    end
    
    def self.event_class_by_type
      @event_class_by_type ||= {}
    end
    
    def self.new(row, opts)
      return nil if record.nil?
      
      if self == PGQ::Event
        type = row[type_index]
        event_class = event_class_by_type[type]
        return event_class.new(row, opts) if event_class
      end
      
      super
    end
    
    def initialize(row, opts)
      @connection = opts[:connection]
      @batch_id   = opts[:batch_id]
      
      self.class.columns.each_with_index do |column, i|
        instance_variable_set("@#{column}".to_sym, row[i])
      end
      
      @id   = @id.to_i
      @txid = @txid.to_ix
      @time = Time.local(*ParseDate.parsedate(@time)) unless @time.nil?
    end
    
    def fail!(reason)
      connection.exec("SELECT pgq.event_failed(#{batch_id}, #{id}, #{connection.quote(reason)})")
    end
    
    def retry!(seconds)
      connection.exec("SELECT pgq.event_retry(#{batch_id}, #{id}, #{seconds})")
    end
        
  private
    def self.columns
      [:id, :time, :txid, :retry, :type, :data, :extra1, :extra2, :extra3, :extra4]
    end

    def self.type_index
      @type_index ||= columns.index(:type)
    end

  public
     attr_reader *columns
  end
end
