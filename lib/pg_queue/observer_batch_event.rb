module PGQueue
  class ObserverBatchEvent < BatchEvent
    attr_reader :old_row, :new_row
    
    event_type        'trigger_observer'
    alias_ev_fields   :table_name => :ev_data, 
                      :operation  => :ev_extra1 
    
    def initialize(*args)
      result = super
      parse_row_fields
      result
    end
    
  private
    def parse_row_fields
      @new_row = {}
      raw_new_row = ev_extra3 ? ev_extra3.dup : ''

      if operation == 'UPDATE'
        @old_row = {} 
        raw_old_row = ev_extra2 ? ev_extra2.dup : ''
      else
        raw_old_row = nil
      end

      self.columns_for_table.each do |col|
        @old_row[col], raw_old_row = shift_col(raw_old_row) if raw_old_row
        @new_row[col], raw_new_row = shift_col(raw_new_row)
      end
    end
    
    def shift_col(raw_row)
      delim = (raw_row.first == '"') ? /[^\\]"/ : ','
      val, remainder = raw_row.split(delim, 2)
      val = val[1,-2] if val.first == '"'
      [val, remainder]
    end
    
    def self.columns_for_table(table_name)
      @table_columns ||= {}
      @table_columns[table_name] ||= get_columns(table_name)
    end
    
    def self.get_columns(table_name)
      connection.select_values("SELECT column_name from information_schema.columns WHERE table_name = '#{table_name}'")
    end
  end
end
