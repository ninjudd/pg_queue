require 'rubygems'
require 'test/unit'
require 'shoulda'
require 'mocha'

$LOAD_PATH.unshift("#{File.dirname(__FILE__)}/../lib")
$LOAD_PATH.unshift(File.dirname(__FILE__))
require 'pg_queue'
require 'pg_queue/batch'
require 'pg_queue/batch_event'
require 'pg_queue/observer_batch_event'

class Test::Unit::TestCase
end
