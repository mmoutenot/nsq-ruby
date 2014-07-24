require_relative '../../spec_helper'

describe Nsq::Utility do
  describe '#nsq_timestamp_to_time' do
    it 'Translates a nanosecond timestamp to Ruby time object' do
      expect(
        described_class.nsq_timestamp_to_time(1406222539363434696)
      ).to be_between(
        Time.new(2014, 7, 24, 13, 22, 19.363434740),
        Time.new(2014, 7, 24, 13, 22, 19.363434760)
      ).inclusive # Account for one decimal of floating point rounding error
    end
  end
end
