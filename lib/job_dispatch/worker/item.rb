# encoding: UTF-8

require 'active_support/dependencies'

module JobDispatch
  class Worker

    #
    # This represents a unit of work to be done. It will be serialised to Mongo database
    #
    class Item
      attr_accessor :job_id
      attr :target
      attr :method
      attr :params
      attr :result
      attr :status

      def initialize(target, method, *params)
        @target, @method, @params = target, method, params
      end

      # execute the method on the target with the given parameters
      # This will capture standard exceptions for return over network.
      def execute
        begin
          JobDispatch.logger.info "Worker executing job #{job_id}: #{target}.#{method}"
          Thread.current["JobDispatch::Worker.job_id"] = job_id
          @klass = target.constantize
          @result = @klass.__send__(method.to_sym, *params)
          @status = :success
        rescue StandardError => ex
          @result = {
              class: ex.class.to_s,
              message: ex.to_s,
              backtrace: ex.backtrace,
          }
          @status = :error
        ensure
          Thread.current["JobDispatch::Worker.job_id"] = nil
          JobDispatch.logger.info "Worker completed job #{job_id}: #{target}.#{method}, status: #{@status}"
        end
      end
    end
  end

end
