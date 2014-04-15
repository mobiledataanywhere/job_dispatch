# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'job_dispatch/version'

Gem::Specification.new do |spec|
  spec.name          = "job_dispatch"
  spec.version       = JobDispatch::VERSION
  spec.authors       = ["Matt Connolly"]
  spec.email         = ["matt@mobiledataanywhere.com"]
  spec.description   = %q{Job Dispatch to workers via ZeroMQ}
  spec.summary       = %q{Job Dispatch is a gem for dispatching jobs to workers in an asynchronous manner. Job Dispatch does not require any specific database. Jobs are dispatched using ZeroMQ messages and workers can be implemented in any language.}
  spec.homepage      = ""
  spec.license       = "MIT"

  spec.files         = `git ls-files`.split($/)
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = ["lib"]

  spec.add_runtime_dependency "rbczmq", "~> 1.7"
  spec.add_runtime_dependency 'nullobject'
  spec.add_runtime_dependency 'json'
  spec.add_runtime_dependency 'text-table', '~> 1.2.3'
  spec.add_runtime_dependency 'activesupport', '>= 3.0.0'

  spec.add_development_dependency "bundler", "~> 1.3"
  spec.add_development_dependency "rspec", "~> 2.14"
  spec.add_development_dependency "simplecov", "~> 0.8"
  spec.add_development_dependency "timecop", "~> 0.7"
  spec.add_development_dependency "factory_girl", "~> 4.3.0"
  spec.add_development_dependency "rake"
end
