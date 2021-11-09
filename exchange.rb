class Exchange
  def initialize(type, name)
    @type = type
    # {bind_key: [queue1, queue2]}
    @clients = Hash.new([])
    # 记录交换机名和交换机的键值对关系
    @@exchanges ||= {}
    raise "已存在交换机#{name}，无法创建" if @@exchanges[name]
    @@exchanges[name] = self
  end

  # 绑定队列
  def bind(queue, bind_key: '')
    @clients[bind_key] << queue unless @clients[bind_key].include?(queue)
  end

  # 找出本交换机根据指定key对应的所有队列
  def find_clients(key)
    case @type
    when 'direct'
      @clients[key]
    when 'fanout'
      @clients.values.flatten
    when 'topic'
      # todo:
      []
    else
      []
    end
  end

  def self.exchange(name)
    @@exchanges ||= {}
    r = @@exchanges[name]
    raise '无此交换机' unless r
    p "exchange:#{r}"
    r
  end
end
