require 'lightio'

class LightIO::TCPSocket
  attr_accessor :queues, :exchange
  def queues
    @queues ||= []
  end
end

class TCPSocket
  attr_accessor :queues, :exchange
  def queues
    @queues ||= []
  end
end
