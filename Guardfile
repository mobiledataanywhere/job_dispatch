# A sample Guardfile
# More info at https://github.com/guard/guard#readme

# To use zeus, cmd: 'zeus rspec'
guard 'rspec', cmd: 'rspec' do

  notification :growl, :sticky => false #, :path => '/usr/local/bin/growlnotify'
  notification :terminal_notifier

  watch(%r{^spec/.+_spec\.rb$})
  watch(%r{^lib/(.+)\.rb$})     { |m| "spec/lib/#{m[1]}_spec.rb" }
  watch('spec/spec_helper.rb')  { "spec" }
end
