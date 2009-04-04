$:.unshift(File.dirname(__FILE__))
require 'pg_queue/ticker'
require 'pg_queue/event'
require 'pg_queue/trigger_event'

class PGQueue
  attr_reader :name, :consumer_id, :ticker

  include Enumerable

  def initialize(opts)
    @name        = opts.delete(:name)
    @consumer_id = opts.delete(:consumer_id) || @name
    @ticker      = Ticker.new(opts) 

    # Register the consumer in case it hasn't been registered yet.
    connection.exec("SELECT pgq.register_consumer(#{quote(name)}, #{quote(consumer_id)})")
  end

  def connection
    ticker.connection
  end
  
  def quote(string)
    connection.quote(string)
  end

  def each
    batch_id = connection.query("SELECT pgq.next_batch('#{name}', '#{consumer_id}')").first
    return unless batch_id
    
    connection.exec("SELECT pgq.get_batch_events(#{batch_id})").each do |row|
      yield Event.new(row, :connection => connection, :batch_id => batch_id)
    end
    connection.exec("SELECT pgq.finish_batch(#{batch_id})")
  end

  def install_trigger_observer(opts)
    raise 'cannot install trigger without table_name' unless opts[:table_name]
    opts[:operations] ||= ['INSERT', 'UPDATE', 'DELETE']
    trigger_name = "observe_#{opts[:table_name]}_for_#{name}"
    connection.exec %{
      CREATE OR REPLACE FUNCTION #{trigger_name}() RETURNS TRIGGER AS $$        
        BEGIN
          pgq.insert_event(#{connection.quote(name)}, 'trigger', TG_TABLE_NAME, TG_OP, OLD::text, NEW::text, NULL);
          RETURN NULL;
        END
      $$ LANGUAGE plpgsql;
      CREATE TRIGGER #{trigger_name} AFTER #{opts[:operations].join(' OR ')} ON search_profiles
        FOR EACH ROW EXECUTE PROCEDURE #{trigger_name}();
    }
  end

  module ActiveRecordExtension
    def pgq(name, opts = {})
      opts[:name] = name
      opts[:connection] ||= connection.raw_connection
      opts[:db]         ||= connection.instance_variable_get(:@config)
      PGQueue.new(opts)
    end

    def install_pgq_observer(name, opts = {})
      queue = pgq(name, opts)
      queue.install_trigger_observer(opts.merge(:table_name => table_name))
    end
  end
end

ActiveRecord.extend(PGQueue::ActiveRecordExtension) if defined?(ActiveRecord)
