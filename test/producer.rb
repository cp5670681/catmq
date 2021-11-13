lib = File.expand_path('../../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'catmq'

client = Catmq::Client.new
client.create_exchange('direct', 'exchange1')
client.exchange_bind_queue('exchange1', 'queue1', 'key1')
client.bind_exchange('exchange1')
t1 = Time.now.to_f
1000.times do |index|
  client.publish('key1', "hello #{index}")
end
puts "花时间#{Time.now.to_f - t1}"

