module Catmq
  class Exchange
    attr_accessor :type, :queues
    
    @exchanges ||= {}
    class << self
      attr_accessor :exchanges

      # 根据交换机的名称，获取对应的交换机
      # @param [String] name 交换机的名称
      def exchange(name)
        r = exchanges[name]
        raise ::Catmq::ExchangeNotFoundError unless r
        r
      end
    end

    # @param [String] type 交换机的类型
    # @param [String] name 交换机的名称
    def initialize(type, name)
      # 创建相同的交换机时，返回之前的交换机
      return self.class.exchanges[name] if self.class.exchanges[name]
      self.type = type
      # {binding_key: [queue1, queue2]}
      self.queues = {}
      # 记录交换机名和交换机的键值对关系
      self.class.exchanges[name] = self
    end

    # 绑定队列
    def bind(queue, bind_key: '')
      self.queues[bind_key] ||= []
      self.queues[bind_key] << queue unless self.queues[bind_key].include?(queue)
    end

    # 找出本交换机根据指定key对应的所有队列
    def find_clients(key)
      case self.type
      when 'direct'
        self.queues[key]
      when 'fanout'
        self.queues.values.flatten
      when 'topic'
        # todo:
        []
      else
        []
      end
    end

  end
end
