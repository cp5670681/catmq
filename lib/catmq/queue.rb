module Catmq
  class Queue
    attr_accessor :name, :queue, :clients
    def initialize(name)
      self.name = name
      self.queue = ::Queue.new
      # 当前队列连接了哪些客户端
      self.clients = []
      # 记录队列名和队列的键值对关系
      @@queues ||= {}
      @@queues[name] = self
    end

    # 不停发送消息给随机一个客户端，直到队列为空
    def send_to_consumer
      p 'send_to_consumer'
      p self.clients
      while true
        random_client = self.clients.sample
        if random_client
          random_client.write(self.pop)
        else
          break
        end
      end
    rescue ThreadError => e
      p "队列为空#{e}"
    end

    def push(obj)
      puts "#队列：#{self.name}入队：#{obj}"
      self.queue.push(obj)
      puts "入队后队列长度:#{self.queue.length}"
      puts "当前clients:#{self.clients}"
      self.send_to_consumer
      p 'end_push'
    end

    def pop
      obj = self.queue.pop(true)
      puts "#队列：#{self.name}出队：#{obj}"
      obj
    end

    def self.queue(name)
      @@queues ||= {}
      r = @@queues[name] || self.new(name)
      p "queue:#{r}"
      r
    end
  end
end
