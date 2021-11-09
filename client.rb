require 'socket'
require 'json'

class Client
  def initialize(hostname = 'localhost', port = 2000)
    @client = TCPSocket.open(hostname, port)
  end

  # 客户端关闭连接
  def close
    @client.close
    # 把连接绑定的队列和连接解绑
    if self.queues
      self.queues.each do |queue|
        queue.clients.delete(self)
      end
    end
  end

  def publish(routing_key, message)
    data = {
      type: 'publish',
      params: {
        routing_key: routing_key,
        message: message
      }
    }
    self._send(data.to_json)
  end

  def create_exchange(type, name)
    data = {
        type: 'create_exchange',
        params: {
            type: type,
            name: name
        }
    }
    self._send(data.to_json)
  end

  def bind_exchange(name)
    data = {
        type: 'bind_exchange',
        params: {
            name: name
        }
    }
    self._send(data.to_json)
  end

  def exchange_bind_queue(exchange_name, queue_name, binding_key)
    data = {
        type: 'exchange_bind_queue',
        params: {
            exchange_name: exchange_name,
            queue_name: queue_name,
            binding_key: binding_key
        }
    }
    self._send(data.to_json)
  end

  def bind_queue(queue_name)
    data = {
      type: 'bind_queue',
      params: {
        queue_name: queue_name
      }
    }
    self._send(data.to_json)
    loop do
      line = @client.gets
      unless line
        p '从服务器断开连接'
        break
      end
      p "接收消息#{line}"
    end
  end

  def _send(message)
    @client.puts(message)
  end

end



