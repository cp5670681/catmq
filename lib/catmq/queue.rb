module Catmq
  class Queue
    attr_accessor :name, :queue, :clients

    @queues ||= {}
    class << self
      attr_accessor :queues
      def queue(name)
        queues[name] || self.new(name)
      end
    end

    def initialize(name)
      # 创建相同的队列时，返回之前的队列
      return self.class.queues[name] if self.class.queues[name]
      self.name = name
      self.queue = ::Queue.new
      # 当前队列连接了哪些客户端
      self.clients = []
      # 记录队列名和队列的键值对关系
      self.class.queues[name] = self
    end

    # 不停发送消息给随机一个客户端，直到队列为空
    def send_to_consumer
      p 'send_to_consumer'
      p self.clients
      while true
        random_client = self.clients.sample
        if random_client
          ::Catmq::Agreement.new(random_client).send_message(self.pop)
        else
          break
        end
      end
    rescue ThreadError => e
      p "队列为空#{e}"
    end

    def push(obj)
      puts "#队列：#{self.name}入队：#{obj}"
      # 添加消息的到期时刻
      self.queue.push(obj.merge('expire' => (Time.now + obj['ttl'] / 1000.0).to_f)) if obj['ttl']
      puts "入队后队列长度:#{self.queue.length}"
      puts "当前clients:#{self.clients}"
      self.send_to_consumer
      p 'end_push'
    end

    def pop
      obj = self.queue.pop(true)
      # 如果消息过期了，则递归出队下一个消息
      if obj['expire'] && (obj['expire'] < Time.now.to_f)
        p "#{obj}消息过期"
        return self.pop
      end
      puts "#队列：#{self.name}出队：#{obj}"
      obj
    end

  end
end
