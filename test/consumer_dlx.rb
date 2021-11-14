lib = File.expand_path('../../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'catmq'

c = Catmq::Client.new
c.bind_queue('dlx.queue') do |res|
  puts res
end
c.close

