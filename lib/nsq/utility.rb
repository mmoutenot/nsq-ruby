module Nsq
  class Utility
    class << self
      def nsq_timestamp_to_time(nsq_timestamp)
        Time.at(
          nsq_timestamp / (10**9),
          nsq_timestamp % (10**9).to_f / 1000
        )
      end
    end
  end
end
