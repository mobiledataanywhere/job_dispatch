source 'https://rubygems.org'

# Specify your gem's dependencies in job_dispatch.gemspec
gemspec

# required for testing rubinius on travis:
platforms :rbx do
  gem 'rubysl', '~> 2.0'
  gem 'rubinius', '~> 2.0'
  gem "rubinius-coverage", github: "rubinius/rubinius-coverage"
end

group :test do
  gem 'guard'
  group :mac do
    gem 'growl'
    gem 'guard-rspec'
    gem 'terminal-notifier-guard'
  end
end
