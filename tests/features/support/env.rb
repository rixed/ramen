require 'rspec'
require 'open3'
require 'fileutils'
require 'tmpdir'

$daemon_pids = {}

def kill_ramens ()
  for again in 1..10 do
    $daemon_pids.each do |cmd, pid|
      begin
        if Process.waitpid(pid, Process::WNOHANG).nil? then
          sig = again < 4 ? 'INT' : 'KILL'
          Process.kill(sig, pid)
          if again > 1 then
            puts "Process #{cmd} didn't react to signal #{sig}, will retry"
          end
        else
          $daemon_pids.delete(pid)
        end
      rescue Errno::ECHILD
        #puts "Process #{cmd} is dead already; Good boy!"
        $daemon_pids.delete(pid)
      rescue Exception => e
        puts "got exception #{e.class}:#{e.message}, proceeding."
      end
    end
    if not $daemon_pids.empty?
      sleep 0.3
    end
  end
  $daemon_pids = {}
end

at_exit do
  kill_ramens()
end

Before do |scenario|
  $prev_wd = Dir.getwd
  $tmp_dir = Dir.mktmpdir('ramen_cucumber_tests_')

  # Look for ramen in src/
  src_dir = Pathname($prev_wd +'/'+ scenario.location.file).dirname +
            "../../src"
  ENV['PATH'] = "#{src_dir}:#{ENV['PATH']}"

  # Reset some ENV:
  ENV['RAMEN_DIR'] = $tmp_dir + '/ramen_dir'
  # Clear this or we might have longer than expected backtraces on stderr:
  ENV['OCAMLRUNPARAM'] = nil
  # By default we want a specific setting for experiments:
  ENV['RAMEN_VARIANTS'] = 'TheBigOne=on'
  # Avoid fault injection:
  ENV['RAMEN_FAULT_INJECTION_RATE'] = '0'
  # Archive all worker output right from the beginning:
  ENV['RAMEN_INITIAL_EXPORT'] = '300'
  # No worker should be allowed to tell what he has seen:
  ENV['RAMEN_KILL_AT_EXIT'] = '1'
  # Most tools authenticate to confserver as $USER:
  ENV['USER'] = 'TESTER'

  # If we do this globally then cucumber fails to find the features, so we
  # cheat by doing this in this hook:
  Dir.chdir $tmp_dir
end

After do |scenario|
  kill_ramens()
  Dir.chdir $prev_wd
  if scenario.failed?
    puts "All the mess is still in #{$tmp_dir} for investigation"
  else
    FileUtils.rm_rf($tmp_dir)
  end
end

# Let's make human lists as string easily splittable:
class String
  def list_split
    self.split(/ +and +|, *| +/)
  end
end

# Small helper to run some program with some arguments and return a hash
# of the result (with stdout, stderr and exit code)
def exec(file, args)
  cmd = "#{file} #{args}"
  stdout, stderr, status = Open3.capture3(cmd)
  { 'stdout' => stdout,
    'stderr' => stderr,
    'status' => status.to_i }
end

class Filter
  include RSpec::Matchers

  def initialize(description)
    # description is supposed to be a string taken from the scenario
    if description =~ /^no$/ then
      @min = @max = 0
    elsif description =~ /^a +few$/ then
      @min = 1
      @max = 20
    elsif description =~ /^(?:(?:a +)?lots?(?: +of)?|many)$/ then
      @min = 20
      @max = 300
    elsif description =~ /^some$/ then
      @min = 1
      @max = 1000
    elsif description =~ /^(\d+)$/ then
      @min = @max = $1.to_i
    elsif description =~ /^less +than +(\d+)$/ then
      @min = 0
      @max = $1.to_i - 1
    elsif description =~ /^more +than +(\d+)$/ then
      @min = $1.to_i + 1
      @max = 1000
    elsif description =~ /^between +(\d+) +and +(\d+)$/ then
      @min = $1.to_i
      @max = $2.to_i
    else
      fail ArgumentError, description
    end
  end

  def check(q)
    expect(q).to be_between(@min, @max).inclusive
  end
end
