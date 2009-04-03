require 'rubygems'
require 'test/unit'
require 'shoulda'
require 'mocha'
require 'pp'

$LOAD_PATH.unshift("#{File.dirname(__FILE__)}/../lib")
$LOAD_PATH.unshift(File.dirname(__FILE__))
require 'pg_queue'
require 'pgq/batch'
require 'pgq/batch_event'
require 'pgq/observer_batch_event'

class Test::Unit::TestCase
end
