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
      Exchange.exchange(params['exchange_name']).bind(MqQueue.queue(params['queue_name']), bind_key: params['binding_key'])
    end
  end

  def router(key, &block)
    @routers[key] = block
  end

  def receive_from_client(socket)
    len = socket.readpartial(5).to_i
    data = socket.readpartial(len)
    res = JSON.parse(data)
    p res
    params = res['params']
    @routers[res['type']]&.call(socket, params)
  rescue EOFError, Errno::ECONNRESET
    _, port, host = socket.peeraddr
    puts "*** #{host}:#{port} disconnected"
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
    # 接收客户端连接
    loop do
      socket = server.accept
      _, port, host = socket.peeraddr
      puts "accept connection from #{host}:#{port}"
      # Thread.new do
      #   receive_from_client(client)
      # end
      LightIO::Beam.new(socket) do |socket|
        while true
          receive_from_client(socket)
        end
      end
    end
  end
end

Server.new.run














