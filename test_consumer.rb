require './client'

c = Client.new
c.bind_queue('queue1')
c.close

