# Some of the steps perform a change while others check that some changes have
# happened. Steps which perform a change are descriptions of the desired state,
# while steps that check uses the modal "must".

Given /(.*) must be in the path/ do |executable|
  found = ENV['PATH'].split(":").any? {|p| File.executable? (p+'/ramen')}
  expect(found).to eq true
end

Given /the environment variable (.*) is set(?: to (.*))?/ \
do |envvar, opt_val|
  if ENV[envvar].nil? and opt_val.nil? then
    val =
      case envvar
        when /RAMEN_LIBS/
          ENV['top_dir'] + '/bundle'
        when /RAMEN_DEBUG/
          '1'
        when /RAMEN_COLORS/
          'never'
        else
          fail(StandardError.new("No idea what to set #{envvar} to"))
      end
    ENV[envvar] = val
  else
    if not opt_val.nil? then
      ENV[envvar] = opt_val
    end
  end
end

Given /the environment variable (.*) is not (?:set|defined)/ do |envvar|
  ENV[envvar] = nil
end

Given /the environment variable (.*) must (not )?be (?:set(?: to (.*))|defined)/ \
do |envvar, unset, opt_val|
  if unset then
    expect(ENV[envvar]).to equal nil
  else
    if opt_val.nil? then
      expect(ENV[envvar]).to be_truthy
    else
      expect(ENV[envvar]).to eq opt_val
    end
  end
end

When /I run (.*) with no argument/ do |executable|
  @output ||= {}
  @output[executable] = exec(executable, '')
end

When /I run (.*) with arguments? (.*)/ do |executable, args|
  @output ||= {}
  @output[executable] = exec(executable, args)
  #puts @output[executable]['stderr']
  #puts @output[executable]['stdout']
end

Then /^([^ ]*) must print (.*) lines?(?: on (std(?:out|err)))?\.?/ \
do |executable, quantity, out|
  out = 'stdout' if out.nil?
  filter = Filter.new(quantity)
  if quantity == 'no' and out == 'stderr' then
    puts @output[executable][out]
  end
  filter.check(@output[executable][out].lines.count)
end

Then /^([^ ]*) must exit with status (not )?(\d)/ do |executable, neg, status|
  exp = status.to_i
  got = @output[executable]['status']
  if neg then
    expect(got).not_to equal exp
  else
    expect(got).to equal exp
  end
end

Given /a file (.*) with(?: perms (0[0-7]{3}) and)? content/ do |file_name, perms, file_content|
  file_name = $tmp_dir + '/' + file_name
  FileUtils.mkdir_p File.dirname(file_name)
  File.open(file_name, "w+") do |f| f.write file_content end
  if perms then
    FileUtils.chmod perms.to_i(base=8), file_name
  end
end

Given /no files? (ending with|starting with|named) (.*) (?:is|are) present in (.*)/ \
do |condition, like, dir|
  Dir[$tmp_dir + '/' + dir + '/*'].each do |f|
    File.delete(f) if
      case condition
        when /ending with/
          f.end_with? like
        when /starting with/
          f.star_with? like
        when /named/
          f == like
      end
  end
end

Given /(a|no) file named (.*) must be present in (.*)/ \
do |presence, name, dir|
  file_name = $tmp_dir + '/' + dir + '/' + name
  expect(File.exist? file_name).to be (presence == 'a')
end

Then /(?:an? )?(executable )?files? (.*) must exist/ \
do |opt_file_type, files|
  files.list_split.each do |f|
    expect(File.exist? f).to be true
    expect(
      case opt_file_type
        when /executable/
          File.executable? f
        when /readable/
          File.readable? f
        when /writable/
          File.writable? f
      end).to be true
  end
end

Then /^([^ ]*) must produce( executables?)? files? (.*)/ \
do |executable, opt_file_type, files|
  step "#{executable} must print a few lines on stdout"
  step "#{executable} must print no line on stderr"
  step "#{executable} must exit with status 0"
  files.list_split.each do |f|
    step "a#{opt_file_type} file #{f} must exist"
  end
end

Then /^([^ ]*) must fail gracefully\.?$/ do |executable|
  step "#{executable} must exit with status not 0"
  step "#{executable} must print a few lines on stderr"
end

Then /^([^ ]*) must (?:exit|terminate) gracefully\.?$/ do |executable|
  step "#{executable} must exit with status 0"
  step "#{executable} must print no line on stderr"
end

Given /(.*\.ramen) is compiled(?: as (.*))?$/ do |source, prog_name|
  if prog_name then
    `ramen compile #{source} --as #{prog_name}`
  else
    `ramen compile #{source}`
  end
end

Given /^(.*) are compiled/ do |programs|
  programs = programs.list_split
  programs.each do |p|
    step "#{p} is compiled"
  end
end

Given /^(ramen .*) is started$/ do |cmd|
  if $daemon_pids[cmd].nil?
    step "the environment variable RAMEN_DIR is set"
    # Cannot daemonize or we won't know the actual pid:
    $daemon_pids[cmd] = Process.spawn(cmd)
  end
end

Given /^(.*) is run every (\d+)? ?seconds?\.?$/ do |cmd, delay|
  if delay.nil? or delay < 1 then
    delay = 1
  end
  cmd = "while sleep #{delay}; do #{cmd}; done"
  if $daemon_pids[cmd].nil?
    $daemon_pids[cmd] = Process.spawn(cmd)
  end
end

Given /the whole gang is started$/ do
  step "ramen must be in the path"
  step "the environment variable HOSTNAME is set to TEST"
  step "the environment variable OCAMLRUNPARAM is set to b"
  step "the environment variable RAMEN_CONFSERVER is set to localhost:29340"
  step "the environment variable RAMEN_REPORT_PERIOD is set to 1"
  step "the environment variable RAMEN_LIBS is set"
  step "the environment variable RAMEN_PATH is not defined"
  step "the environment variable RAMEN_DEBUG is set"
  step "the environment variable RAMEN_COLORS is set"
  step "the environment variable RAMEN_KEEP_TEMP_FILES is set to 1"
  step "ramen start --insecure 127.0.0.1:29340 --no-examples --allocs is started"
  step "the environment variable RAMEN_DEBUG is not defined"
  # Wait to make sure confserver is accepting connections:
  step "I wait 1 second"
end

Given /^user (\w+) is defined with (\w+) perms$/ do |username, perms|
  identity_cont=`ramen useradd #{username}`
  `ramen usermod #{username} -r #{perms}`
  pub=/"client_public_key"[[:space:]]*:"([^"]*)"/.match(identity_cont)[1].gsub("\\", "")
  priv=/"client_private_key"[[:space:]]*:"([^"]*)"/.match(identity_cont)[1].gsub("\\", "")
  steps %{
    Given a file identity with perms 0400 and content
    """
    #{identity_cont}
    """
    Given a file priv with perms 0400 and content
    """
    #{priv}
    """
    Given a file pub with perms 0400 and content
    """
    #{pub}
    """
  }
end

Then /^(ramen .*) must still be running/ do |cmd|
  expect($daemon_pids[cmd]).to be_truthy
  ps_out = `ps -p #{$daemon_pids[cmd]} -o comm`
  expect(ps_out).to match(/^ramen\b/)
end

Given /^no worker must be running$/ do
  `ramen ps`.lines.length == 0
end

Given /^(?:the )?workers? (.*) must( not)? be running/ do |workers, not_run|
  workers = workers.list_split
  re = Regexp.union(workers.map{|w| /^TEST\t#{Regexp.escape(w)}\t/})
  l = `ramen ps`.lines.select{|e| e =~ re}.length
  if not_run
    expect(l).to be == 0
  else
    expect(l).to be == workers.length
  end
end

Given /^(?:the )?programs? (.*) must( not)? be running/ do |programs, not_run|
  programs = programs.list_split
  re = Regexp.union(programs.map{|w| /^TEST\t#{Regexp.escape(w)}\//})
  l = `ramen ps`.lines.select{|e| e =~ re}.uniq.length
  if not_run
    expect(l).to be == 0
  else
    expect(l).to be == programs.uniq.length
  end
end

Given /^no (?:program|worker)s? (?:is|are) running/ do
  `ramen ps`.lines.select{|e| e =~ /^TEST\t([^\t]+)\/[^\t\/]+\t/}.uniq.each do |e|
    prog = $1
    `ramen kill "#{prog}"`
  end
end

Given /^(?:the )?programs? (.*) (?:is|are) not running/ do |programs|
  re = Regexp.union(programs.list_split.map{|w| /^TEST\t(#{Regexp.escape(w)})\//})
  l = `ramen ps`.lines.select{|e| e =~ re}.uniq.each do |e|
    prog = $1
    `ramen kill "#{prog}"`
  end
end

Given /^(?:the )?programs? (.*) (?:is|are) running/ do |programs|
  def running?(prog)
    ps = `ramen ps`.lines.map do |l|
      l =~ /^TEST\t([^\t]+)\/[^\t\/]+\t/
      $1
    end
    ps.include? prog
  end
  programs.list_split.each do |prog|
    if not running? prog
      expect(system("ramen run '#{prog}'")).to eq true
      # wait until it is actually running
      while not running? prog
        sleep 0.3
      end
    end
  end
end

Then /^after max (\d+) seconds? (.+)$/ do |max_delay, what|
  done = false # work around the bact we cannot return from a step
  max_delay = max_delay.to_i
  for again in 1..max_delay do
    begin
      step what
      done = true
      break
    rescue Exception => e
      puts "got exception #{e}, retrying"
      sleep 1
    end
  end
  # One last time with no safety net:
  if not done then
    step what
  end
end

Then /^([^ ]*) must (not )?mention "(.*)"(?: on (std(?:out|err)))?\.?/ \
do |executable, not_, what, out|
  out = 'stdout' if out.nil?
  output = @output[executable][out]
  if not_ then
    expect(output).not_to match(/#{Regexp.escape(what)}/)
  else
    expect(output).to match(/#{Regexp.escape(what)}/)
  end
end

Then /^([^ ]*) must (not )?warn about "(.*)".?/ \
do |executable, not_, what|
  step "#{executable} must #{not_}mention \"#{what}\" on stderr"
end

When /I wait (\d+) seconds?/ do |n|
  sleep n.to_i
end

Then /^the query below against (.*) must return (.*)$/ \
do |dbfile, answer, query|
  # Database might be locked by the alerter, thus the sleep. Anyway, we
  # want to wait until the async alerter tried to notify:
  out = `sleep 2 && sqlite3 #{dbfile} '.separator ,' '#{query}'`
  expect(out).to match(/\b#{Regexp.escape(answer)}\b/)
end

Given /^configuration key (.*) is set to:$/ do |key, value|
  # exec does not handle newlines in arguments for some reason:
  value = value.gsub("\n", " ")
  step "I run ramen with arguments confclient --key '#{key}' --value '#{value}'"
  step "ramen must exit gracefully."
end
