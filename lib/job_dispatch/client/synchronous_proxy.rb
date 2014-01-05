# encoding: UTF-8

module JobDispatch

  # This is a simple class for making synchronous calls to the Job Queue dispatcher.
  class Client

    class SynchronousProxy < Proxy

      def method_missing(method, *args)
        job_spec = @client.enqueue(queue: queue, target: @target, method: method.to_s, parameters: args)
        completed_job = @client.notify(job_spec["job_id"])
        if completed_job.nil?
          raise ProxyError.new("Internal error! There should not be a nil response from the broker.")
        end
        result = completed_job["job"] && completed_job["job"]["result"]
        case completed_job["status"]
          when "failed"
            raise ProxyError.new("Job failed: #{result}", completed_job)
          when "completed"
            return result
          else
            raise ProxyError.new("Notify should not return for a pending or in progress job!")
        end
      end

    end
  end
end
