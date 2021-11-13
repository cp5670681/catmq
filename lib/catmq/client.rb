require 'socket'
require 'json'
require 'lightio'

module Catmq
  class Client
    def initialize(hostname = 'localhost', port = 2000)
      @socket = TCPSocket.open(hostname, port)
    end

    # 客户端关闭连接
    def close
      @socket.close
      # 把连接绑定的队列和连接解绑
      # if self.queues
      #   self.queues.each do |queue|
      #     queue.clients.delete(self)
      #   end
      # end
    end

    def publish(routing_key, message)
      data = {
        routing_key: routing_key,
        message: message
      }
      ::Catmq::Agreement.new(@socket).send_message(data, router: 'publish')
    end

    def create_exchange(type, name)
      data = {
        type: type,
        name: name
      }
      ::Catmq::Agreement.new(@socket).send_message(data, router: 'create_exchange')
    end

    def bind_exchange(name)
      data = {
        name: name
      }
      ::Catmq::Agreement.new(@socket).send_message(data, router: 'bind_exchange')
    end

    def exchange_bind_queue(exchange_name, queue_name, binding_key)
      data = {
        exchange_name: exchange_name,
        queue_name: queue_name,
        binding_key: binding_key
      }
      ::Catmq::Agreement.new(@socket).send_message(data, router: 'exchange_bind_queue')
    end

    def bind_queue(queue_name, &block)
      data = {
        queue_name: queue_name
      }
      ::Catmq::Agreement.new(@socket).send_message(data, router: 'bind_queue')
      ::Catmq::Agreement.new(@socket).receive do |res|
        block.call(res)
      end
    end

    def _send(message)
      ::Catmq::Agreement.new(@socket).send_message(message)
    end

  end
end
