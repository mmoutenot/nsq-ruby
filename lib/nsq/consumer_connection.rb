require_relative 'connection'
require_relative 'logger'

module Nsq
  class ConsumerConnection < Connection
    attr_accessor :max_in_flight
    attr_reader :presumed_in_flight

    def initialize(host, port, topic, channel, queue)
      @queue = queue
      @presumed_in_flight = 0
      @max_in_flight = 1
      @topic = topic
      @channel = channel

      super(host, port)
    end


    def subscribe
      info "#{@port} Subscribing"
      sub(@topic, @channel)
      re_up_ready
    end


    def after_connect_hook
      subscribe
    end
  end
end
