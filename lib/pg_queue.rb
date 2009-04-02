module PGQueue
  ASYNC = 'async' # Used for queue and consumer.

  def pgq_install
    system!("pgqadm.py #{pgq_ini} install", :verbose => true)
  end
  
  def pgq_start
    system!("pgqadm.py -d #{pgq_ini} ticker", :verbose => true)
  end
  
  def pgq_stop
    system!("pgqadm.py -s #{pgq_ini} ticker", :verbose => true)
  end
  
  def pgq_init_async
    execute %{
      SELECT pgq.create_queue('#{ASYNC}');
      SELECT pgq.register_consumer('#{ASYNC}', '#{ASYNC}');

      CREATE OR REPLACE FUNCTION execute_async(text) RETURNS VOID AS $$
        BEGIN
          PERFORM pgq.insert_event('#{ASYNC}', 'execute', $1);
        END
      $$ LANGUAGE plpgsql;

      CREATE OR REPLACE FUNCTION process_async_statements() RETURNS integer AS $$
        DECLARE
          batch_id        integer;
          batch_size      integer;
          r               record;
          status_sequence text;
        BEGIN
          batch_size := 0;
          batch_id := pgq.next_batch('#{ASYNC}', '#{ASYNC}');
    
          IF batch_id IS NOT NULL THEN
            status_sequence := '#{ASYNC}' || '_batch_' || batch_id;
            EXECUTE('CREATE SEQUENCE ' || status_sequence);

            FOR r IN SELECT * from pgq.get_batch_events(batch_id) LOOP
              batch_size := batch_size + 1;              

              EXECUTE(r.ev_data);
              PERFORM nextval(status_sequence);
            END LOOP;
            PERFORM pgq.finish_batch(batch_id);
            EXECUTE('DROP SEQUENCE ' || status_sequence);
          END IF;
          RETURN batch_size;
        END
      $$ LANGUAGE plpgsql;
    }
  end

  def pgq_process_async(opts = {})
    loop do
      begin
        result = select_value("SELECT process_async_statements()").to_i
        puts "#{Time.now.strftime('%Y/%m/%d %H:%M:%S')} Processed batch of size #{result}" if opts[:verbose] and result != 0
      rescue ActiveRecord::StatementInvalid => e
        puts e.message
      end
                  
      sleep(0.25) if result == 0
    end
  end

private

  def pgq_ini
    infile  = RAILS_ROOT + '/config/pgq.ini.template'
    outfile = RAILS_ROOT + '/config/pgq.ini'
    
    db = @config # for ERB binding
    File.open(outfile, 'w') do |file|
      file << ERB.new(IO.read(infile), nil, '<%>').result(binding)
    end
    outfile
  end
end
