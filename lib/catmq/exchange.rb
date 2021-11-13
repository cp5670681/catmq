module Catmq
  class Exchange
    @exchanges ||= {}
    class << self
      attr_accessor :exchanges
      def exchange(name)
        r = exchanges[name]
        raise '无此交换机' unless r
        r
      end
    end

    def initialize(type, name)
      # 创建相同的交换机时，返回之前的交换机
      return self.class.exchanges[name] if self.class.exchanges[name]
      @type = type
      # {bind_key: [queue1, queue2]}
      @queues = {}
      # 记录交换机名和交换机的键值对关系
      self.class.exchanges[name] = self
    end

    # 绑定队列
    def bind(queue, bind_key: '')
      @queues[bind_key] ||= []
      @queues[bind_key] << queue unless @queues[bind_key].include?(queue)
    end

    # 找出本交换机根据指定key对应的所有队列
    def find_clients(key)
      case @type
      when 'direct'
        @queues[key]
      when 'fanout'
        @queues.values.flatten
      when 'topic'
        # todo:
        []
      else
        []
      end
    end

  end
end
