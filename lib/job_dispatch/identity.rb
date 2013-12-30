require 'active_support/core_ext'
require 'active_support/core_ext/object/to_json'

module JobDispatch
  # Identity encapsulates a ZeroMQ socket identity, which is a string of binary characters, typically
  # containing nulls or non-utf8 compatible characters in ASCII-8BIT encoding.
  class Identity

    include Comparable

    attr_reader :identity

    def initialize(identity)
      @identity = identity.to_sym
    end

    def to_s
      @identity.to_s
    end

    def to_str
      @identity.to_str
    end

    def to_hex
      @identity.to_s.bytes.map { |x| '%02x' % x }.join
    end

    def as_json(options={})
      to_hex.as_json(options)
    end

    def to_sym
      @identity
    end

    def hash
      @identity.hash
    end

    def ==(other)
      @identity == other.identity
    end

    def eql?(other)
      self.class == other.class && @identity == other.identity
    end

    def <=>(other)
      @identity <=> other.identity
    end

  end
end
