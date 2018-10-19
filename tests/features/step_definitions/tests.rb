# Some of the steps perform a change while others check that some changes have
# happened. Steps which perform a change are descriptions of the desired state,
# while steps that check uses the modal "must".

Given /(.*) must be in the path/ do |executable|
  found = ENV["PATH"].split(":").any? {|p| File.executable? (p+'/ramen')}
  expect(found).to eq true
end

Given /the environment variable (.*) is set(?: to (.*))?/ \
do |envvar, opt_val|
  if ENV[envvar].nil? and opt_val.nil? then
    val =
      case envvar
        when /RAMEN_BUNDLE_DIR/
          ENV['HOME'] + '/share/src/ramen/bundle'
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
  #puts @output[executable]['stdout']
  #puts @output[executable]['stderr']
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

Then /^([^ ]*) must exit with status (.*)(\d)/ do |executable, cmp, status|
  exp = status.to_i
  got = @output[executable]['status']
  case cmp
    when ''
      expect(got).to equal exp
    when /not|different from/
      expect(got).not_to equal exp
  end
end

Given /a file (.*) with content/ do |file_name, file_content|
  file_name = $tmp_dir + '/' + file_name
  FileUtils.mkdir_p File.dirname(file_name)
  File.open(file_name, "w+") do |f| f.write file_content end
end

Given /no files? (ending with|starting with|named) (.*) (?:is|are) present in (.*)/ \
do |condition, like, dir|
  Dir[$tmp_dir +'/' + dir + '/*'].each do |f|
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

Given /(.*\.ramen) is compiled( as (.*))?$/ do |source, prog_name|
  if prog_name then
    # We want both the program and the binary to have that name, for autoreload
    `ramen compile #{source} --as #{prog_name} -o #{prog_name}.x`
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
    step "the environment variable RAMEN_PERSIST_DIR is set"
    # Cannot daemonize or we won't know the actual pid:
    $daemon_pids[cmd] = Process.spawn(cmd)
  end
end

Given /^no worker must be running$/ do
  `ramen ps`.lines.length == 0
end

Given /^(?:the )?workers? (.*) must( not)? be running/ do |workers, not_run|
  workers = workers.list_split
  re = Regexp.union(workers.map{|w| /^#{Regexp.escape(w)}\t/})
  l = `ramen ps`.lines.select{|e| e =~ re}.length
  if not_run
    expect(l).to be == 0
  else
    expect(l).to be == workers.length
  end
end

Given /^(?:the )?programs? (.*) must( not)? be running/ do |programs, not_run|
  programs = programs.list_split
  re = Regexp.union(programs.map{|w| /^#{Regexp.escape(w)}\t/})
  l = `ramen ps --short`.lines.select{|e| e =~ re}.length
  if not_run
    expect(l).to be == 0
  else
    expect(l).to be == programs.length
  end
end

Given /no (?:program|worker)s? (?:is|are) running/ do
  `ramen ps --short`.lines.select{|e| e =~ /^([^\t]+)\t/}.each do |e|
    prog = $1
    `ramen kill "#{prog}"`
  end
end

Given /(?:the )?programs? (.*) (?:is|are) not running/ do |programs|
  re = Regexp.union(programs.list_split.map{|w| /^(#{Regexp.escape(w)})\t/})
  l = `ramen ps --short`.lines.select{|e| e =~ re}.each do |e|
    prog = $1
    `ramen kill "#{prog}"`
  end
end

Given /(?:the )?programs? (.*) (?:is|are) running/ do |programs|
  running = `ramen ps --short`.lines.map do |l|
    l =~ /^([^\t]+)\t/
    $1
  end
  programs.list_split.each do |prog|
    if not running.include? prog
      expect(system("ramen run '#{prog}.x'")).to eq true
    end
  end
end

Then /^after max (\d+) seconds? (.+)$/ do |max_delay, what|
  done = false # work around the bact we cannot return from a step
  for again in 1..max_delay do
    begin
      step what
      done = true
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
    expect(output).not_to match(/#{what}/)
  else
    expect(output).to match(/#{what}/)
  end
end

When /I wait (\d+) seconds?/ do |n|
  sleep n
end

Then /^the query below against (.*) must return (.*)$/ \
do |dbfile, answer, query|
  # Database might be locked by the notifier, thus the sleep. Anyway, we
  # want to wait until the async notifier tried to notify:
  out = `sleep 2 && sqlite3 #{dbfile} '.separator ,' '#{query}'`
  expect(out).to match(/\b#{answer}\b/)
end
