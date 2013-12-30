require 'simplecov'
SimpleCov.start 'rails'

require 'job_dispatch'

# Requires supporting ruby files with custom matchers and macros, etc,
# in spec/support/ and its subdirectories.
Dir["spec/support/**/*.rb"].each { |f| load f }

require 'factory_girl'
Dir["spec/factories/**/*.rb"].each { |f| load f }

require 'timecop'


RSpec.configure do |config|

  # Run specs in random order to surface order dependencies. If you find an
  # order dependency and want to debug it, you can fix the order by providing
  # the seed, which is printed after each run.
  #     --seed 1234
  #config.order = "random"
end