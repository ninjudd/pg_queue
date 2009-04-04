class PGQueue
  class Ticker
    attr_reader :config, :connection, :db

    def initialize(opts)
      @connection = opts.delete(:connection)
      @config     = self.class.config.merge(opts)
      @db         = config.delete(:db)
    end

    def connection
      @connection ||= PGconn.connect(db[:host], db[:port], nil, nil, db[:database], db[:username], db[:password])
    end

    def install
      system("pgqadm.py #{config_file} install")
      raise 'unable to install pgq ticker' if $?.exitstatus != 0
    end
  
    def start
      system("pgqadm.py -d #{config_file} ticker")
      raise 'unable to start pgq ticker' if $?.exitstatus != 0
    end
    
    def stop
      system("pgqadm.py -s #{config_file} ticker")
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

  private
  
    def config_file
      config[:job_name] ||= "#{db[:host]}-#{db[:port]}-#{db[:database]}"
      filename = "#{config_dir}/#{config[:job_name]}.ini"

      File.open(filename, 'w') do |file|
        file.puts '[pgqadm]'
        file.puts "db = user=#{db[:username]} dbname=#{db[:database]} port=#{db[:port]} host=#{db[:host]} password=#{db[:password]}"
        config.each do |key, value|
          file.puts "#{key} = #{value}"
        end
      end
      filename
    end
  end
end
