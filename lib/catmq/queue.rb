module Catmq
  class Queue
    attr_accessor :name, :queue, :clients, :ttl, :ttl_hash

    DLX_QUEUE_NAME = 'dlx.queue'

    @queues ||= {}
    class << self
      attr_accessor :queues
      def queue(name)
        queues[name] || self.new(name)
      end

      # 死信队列
      def dlx_queue
        self.queue(DLX_QUEUE_NAME)
      end
    end

    # ttl:队列过期时间，单位毫秒
    def initialize(name, ttl: nil)
      # 创建相同的队列时，返回之前的队列
      return self.class.queues[name] if self.class.queues[name]
      self.name = name
      self.queue = ::Queue.new
      # 当前队列连接了哪些客户端
      self.clients = []
      # 记录队列名和队列的键值对关系
      self.class.queues[name] = self
      self.ttl = ttl
      self.ttl_hash = {}
      if ttl
        # 1秒清理一次过期队列消息
        LightIO::Beam.new do
          while true
            self.timing_clear_expire
            LightIO.sleep(1)
          end
        end
      end
    end

    # 不停发送消息给随机一个客户端，直到队列为空
    def send_to_consumer
      p 'send_to_consumer'
      # p self.clients
      while true
        random_client = self.clients.sample
        if random_client
          ::Catmq::Agreement.new(random_client).send_original(self.pop)
        else
          break
        end
      end
    rescue ThreadError => e
      p "队列为空#{e}"
    end

    def push(obj)
      obj_copy = obj.dup
      puts "#队列：#{self.name}入队：#{obj}"
      # 添加消息的到期时刻
      if obj['ttl'] || self.ttl
        ttl = [(obj['ttl'] || Float::INFINITY), (self.ttl || Float::INFINITY)].min
        obj_copy.merge!('expire' => (Time.now + ttl / 1000.0).to_f)
      end
      # 添加队列ttl_hash
      if self.ttl
        self.ttl_hash[(Time.now + self.ttl / 1000.0 + 1).to_i] = obj['uuid']
      end

      self.queue.push(obj_copy)
      puts "入队后队列长度:#{self.queue.length}"
      puts "当前clients:#{self.clients}"
      self.send_to_consumer
      p 'end_push'
    end

    def pop
      obj = self.queue.pop(true)
      puts obj
      # 如果消息过期了，则递归出队下一个消息
      if obj['expire'] && (obj['expire'] < Time.now.to_f)
        p "#{obj}消息过期"
        # 加入死信队列
        obj.delete('ttl')
        obj.delete('expire')
        self.class.dlx_queue.push(obj)
        return self.pop
      end
      puts "#队列：#{self.name}出队：#{obj}"
      obj
    end

    # 清理队列过期消息
    def timing_clear_expire
      uuid = self.ttl_hash[Time.now.to_i]
      return unless uuid
      while true
        obj = self.queue.pop(true)
        break if obj['uuid'] == uuid
        p "清理队列过期消息#{obj['uuid']}"
        # 加入死信队列
        obj.delete('ttl')
        obj.delete('expire')
        self.class.dlx_queue.push(obj)
      end
    rescue ThreadError => e
      p "队列为空#{e}，清理完毕"
    end

  end
end
