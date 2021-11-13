require 'socket'
require 'json'
require 'catmq/queue'
require 'catmq/class_ext'
require 'catmq/exchange'
require 'lightio'

module Catmq
  class Server
    def initialize(port: 2000)
      @port = port
      @routers = {}
    end

    # 绑定路由
    def bind_router
      router 'publish' do |socket, params|
        queues = socket.exchange.find_clients(params['routing_key'])
        queues.each do |queue|
          queue.push(params['message'])
        end
      end
      router 'bind_queue' do |socket, params|
        p 'bind_queue'
        queue = Catmq::Queue.queue(params['queue_name'])
        queue.clients << socket
        socket.queues << queue
        queue.send_to_consumer
      end
      router 'create_exchange' do |socket, params|
        Exchange.new(params['type'], params['name'])
        p '创建交换机成功'
      end
      router 'bind_exchange' do |socket, params|
        socket.exchange = Exchange.exchange(params['name'])
      end
      router 'exchange_bind_queue' do |socket, params|
        p 'exchange_bind_queue'
        Exchange.exchange(params['exchange_name']).bind(Catmq::Queue.queue(params['queue_name']), bind_key: params['binding_key'])
      end
    end

    def router(key, &block)
      @routers[key] = block
    end

    def print_status
      p '=======exchanges'
      p Exchange.exchanges

    end

    def receive_from_client(socket)
      ::Catmq::Agreement.new(socket).receive do |response|
        res = JSON.parse(response)
        params = res['body']
        @routers[res['router']]&.call(socket, params)
      end
    rescue EOFError, Errno::ECONNRESET
      # _, port, host = socket.peeraddr
      puts "*** client disconnected"
      print_status
      # 队列绑定的消息者解绑
      socket.queues.each do |queue|
        queue.clients - [socket]
      end
      socket.close
      raise
    rescue => e
      p e
      puts e.backtrace
    end

    def run
      bind_router
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
        rescue => e
          p e
          puts e.backtrace
        end
      end
    end
  end
end
