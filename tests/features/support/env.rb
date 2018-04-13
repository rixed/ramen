require 'rspec'
require 'open3'
require 'fileutils'
require 'tmpdir'

# Clear this or we might have long, unexpected backtraces on stderr:
ENV['OCAMLRUNPARAM'] = nil

$tmp_dir = Dir.mktmpdir('ramen_cucumber_tests_')
ENV['RAMEN_PERSIST_DIR'] = $tmp_dir + '/ramen_persist_dir'
def kill_ramens ()
  if not $ramen_pid.nil? then
    Process.kill('INT', $ramen_pid)
    Process.waitpid $ramen_pid
    $ramen_pid = nil
  end
end

at_exit do
  kill_ramens()
  if ($!.success?)
    FileUtils.rm_rf($tmp_dir)
  else
    puts "All the mess is still in #{$tmp_dir} for investigation"
  end
end

Before do |scenario|
  # If we do this globally then cucumber fails to find the features, so we
  # cheat by doing this in this hook:
  Dir.chdir $tmp_dir
end

After do |scenario|
  kill_ramens()
  # To simplify reasoning about each individual cases that frequently reuse
  # the same programs, we clean the programs memory between two scenarii:
  FileUtils.rm_rf($tmp_dir + '/ramen_persist_dir/workers')
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
    if description =~ /no/ then
      @min = @max = 0
    elsif description =~ /a few/ then
      @min = 1
      @max = 10
    elsif description =~ /(?:(?:a )?lots?(?: of)?|many)/ then
      @min = 10
      @max = 300
    elsif description =~ /some/ then
      @min = 1
      @max = 1000
    elsif description =~ /(\d)/ then
      @min = @max = $1.to_i
    else
      fail ArgumentError, description
    end
  end

  def check(q)
    expect(q).to be_between(@min, @max).inclusive
  end
end
