require 'csv'

class PGQueue
  class TriggerEvent < Event
    attr_reader :old_record, :new_record
    
    event_type 'trigger'

    alias_method :table_name, :data
    alias_method :operation,  :extra1
    
    def initialize(row, opts)
      result = super
      parse_record_fields
      result
    end
    
  private

    def parse_record_fields
      if extra2
        @old_record = {}
        old_row = CSV::Reader.parse(extra2[1..-2]).to_a.first
      end

      if extra3
        @new_record = {}
        new_row = CSV::Reader.parse(extra3[1..-2]).to_a.first
      end

      columns_for_table(table_name).each do |col|
        @old_record[col] = old_row.shift if old_row
        @new_record[col] = new_row.shift if new_row
      end
    end
    
    def columns_for_table(table_name)
      @columns_for_table ||= {}
      @columns_for_table[table_name.to_s] ||= connection.select_values %{
        SELECT column_name from information_schema.columns WHERE table_name = '#{table_name}'
      }.strip
    end
  end
end
