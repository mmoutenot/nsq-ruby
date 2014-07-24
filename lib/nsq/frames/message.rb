require_relative 'frame'
require_relative '../utility'

module Nsq
  class Message < Frame
    class CannotModifyExpiredMessageError < StandardError
    end
  end
end

module Nsq
  class Message < Frame

    attr_reader :timestamp
    attr_reader :attempts
    attr_reader :id
    attr_reader :body


    def initialize(data, connection)
      super
      @timestamp, @attempts, @id, @body = @data.unpack('Q>s>a16a*')
    end


    def finish
      validate_attempt_to_modify
      connection.fin(id)
    end


    def requeue(timeout = 0)
      validate_attempt_to_modify
      connection.req(id, timeout)
    end


    def touch
      validate_attempt_to_modify
      connection.touch(id)
    end


    private
    def validate_attempt_to_modify
      time = Time.now
      raise CannotModifyExpiredMessageError.new(
        "Can't modify message #{id}; expired at #{timestamp}, now #{time.to_i}#{time.nsec}"
      )
    end


    def has_message_expired?
      max_time = Utility.nsq_timestamp_to_time(
        timestamp + (Connection::NSQ_MESSAGE_TIMEOUT_IN_SECONDS * (10**9))
      )
      Time.now >= max_time
    end
  end
end
