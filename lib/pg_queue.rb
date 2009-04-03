$:.unshift(File.dirname(__FILE__))
require 'pgq/event'
require 'pgq/trigger_event'

class PGQueue
  attr_reader :config, :name, :consumer_id

  def initialize(opts)
    @connection  = opts.delete(:connection)
    @name        = opts.delete(:name)
    @consumer_id = opts.delete(:consumer_id) || @name
    @config      = opts

    # Register the consumer in case it hasn't been registered yet.
    connection.exec("SELECT pgq.register_consumer(#{connection.quote(name)}, #{connection.quote(consumer_id)})")
  end

  def db
    config[:db]
  end

  def connection
    @connection ||= PGconn.connect(db[:host], db[:port], nil, nil, db[:database], db[:user], db[:password])
  end
  
  def each
    batch_id = connection.select_value("SELECT pgq.next_batch('#{name}', '#{consumer_id}')")
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

  def self.install(opts)
    system("pgqadm.py #{config_file(opts)} install")
    raise 'unable to install pgq' if $?.exitstatus != 0
  end
  
  def self.start(opts)
    system("pgqadm.py -d #{config_file(opts)} ticker")
    raise 'unable to start pgq ticker' if $?.exitstatus != 0
  end
  
  def self.stop(opts)
    system("pgqadm.py -s #{config_file(opts)} ticker")
    raise 'unable to stop pgq ticker' if $?.exitstatus != 0
  end

  def self.config
    @config ||= {
      :maint_delay => 10,
      :loop_delay  => 0.1,
      :logfile     => './log/%(job_name)s.log',
      :pidfile     => './log/%(job_name)s.pid',
    }
  end

  class << self
    attr_accessor :config_dir
  end

  def self.config_file(opts)
    opts = config.merge(opts)
    db   = opts.delete(:db)
    opts[:job_name] ||= "#{db[:host]}-#{db[:port]}-#{db[:database]}"
    filename = "#{config_dir}/#{opts[:job_name]}.ini"

    File.open(filename, 'w') do |file|
      file.puts '[pgqadm]'
      file.puts "db = user=#{db[:user]} dbname=#{db[:database]} port=#{db[:port]} host=#{db[:host]} password=#{db[:password]}"
      opts.each do |key, value|
        file.puts "#{key} = #{value}"
      end
    end
    filename
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
      queue.install_trigger_observer(opts.merge(:table_name => table_name)
    end
  end
end

ActiveRecord.extend(PGQueue::ActiveRecordExtension) if defined?(ActiveRecord)
