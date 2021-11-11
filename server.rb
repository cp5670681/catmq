require 'socket'
require 'json'
require './mq_queue'
require './class_ext'
require './exchange'
require 'lightio'

class Server
  def initialize(port: 2000)
    @port = port
    @routers = {}
  end

  # 绑定路由
  def bind_router
    router 'publish' do |client, params|
      queues = client.exchange.find_clients(params['routing_key'])
      queues.each do |queue|
        queue.push(params['message'])
      end
    end
    router 'bind_queue' do |client, params|
      p 'bind_queue'
      queue = MqQueue.queue(params['queue_name'])
      queue.clients << client
      client.queues << queue
      queue.send_to_consumer
    end
    router 'create_exchange' do |client, params|
      Exchange.new(params['type'], params['name'])
      p '创建交换机成功'
    end
    router 'bind_exchange' do |client, params|
      client.exchange = Exchange.exchange(params['name'])
    end
    router 'exchange_bind_queue' do |client, params|
      p 'exchange_bind_queue'
      Exchange.exchange(params['exchange_name']).bind(MqQueue.queue(params['queue_name']), bind_key: params['binding_key'])
    end
  end

  def router(key, &block)
    @routers[key] = block
  end

  def print_status
    p '=======exchanges'
    p Exchange.exchanges

  end

  # 由于tcp的特性，接收的数据可能不完整：或者是一条消息只取到前面部分，或者是一次取到好几条消息的合并
  # 利用生成器模式，每次yield一个完整的消息出去
  # 外部用块遍历，每次得到的就是一个完整的消息了
  def output_res(socket)
    buf = ''
    split = "\n\n"
    while true
      while index = buf.index(split)
        yield JSON.parse(buf[0...index])
        buf = buf[index + split.length...].to_s
      end
      chunk = socket.readpartial(1024)
      buf += chunk
    end
  end

  def receive_from_client(socket)
    output_res(socket) do |res|
      params = res['params']
      @routers[res['type']]&.call(socket, params)
    end
  rescue EOFError, Errno::ECONNRESET
    _, port, host = socket.peeraddr
    puts "*** #{host}:#{port} disconnected"
    print_status
    # 队列绑定的消息者解绑
    socket.queues.each do |queue|
      queue.clients.remove(socket)
    end
    socket.close
    raise
  rescue => e
    p e
    puts e.backtrace
  end

  def run
    bind_router
    # server = TCPServer.open(@port)
    server = LightIO::TCPServer.new('localhost', 2000)
    puts "start server on port #{@port}"
    print_status
    # 接收客户端连接
    while true
      socket = server.accept
      _, port, host = socket.peeraddr
      puts "accept connection from #{host}:#{port}"
      # Thread.new do
      #   receive_from_client(client)
      # end
      LightIO::Beam.new(socket) do |socket|
        receive_from_client(socket)
      end
    end
  end
end

Server.new.run
