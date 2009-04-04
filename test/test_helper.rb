require 'rubygems'
require 'test/unit'
require 'shoulda'
require 'mocha'
require 'pp'

$LOAD_PATH.unshift("#{File.dirname(__FILE__)}/../lib")
$LOAD_PATH.unshift(File.dirname(__FILE__))
require 'pg_queue'

class Test::Unit::TestCase
end
