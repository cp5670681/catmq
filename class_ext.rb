class TCPSocket
  attr_accessor :queues, :exchange
  def queues
    @queues ||= []
  end
end
