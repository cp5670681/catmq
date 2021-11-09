require 'socket'
require 'json'
require './mq_queue'
require './class_ext'
require './exchange'


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

  # 从客户端接收消息
  def receive_from_client(client)
    loop do
      begin
        res = client.gets
        unless res
          p "#{client}已断开"
          client.close
          break
        end
        res = JSON.parse(res)
        p res
        params = res['params']
        @routers[res['type']]&.call(client, params)
      rescue => e
        p '连接异常'
        p e
        p e.backtrace
        sleep 5
      end
    end
  end

  def run
    bind_router
    server = TCPServer.open(@port)
    puts "start server on port #{@port}"
    # 接收客户端连接
    loop do
      client = server.accept
      Thread.new do
        receive_from_client(client)
      end
    end
  end
end

Server.new.run














