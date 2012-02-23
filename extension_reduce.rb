#! /usr/bin/env ruby

collated=Hash.new {|h,k| h[k]=0}
ARGF.each {|line|
  collated[line.chomp]+=1
}
collated.sort_by {|k,v| v}.reverse.each {|k,v|
  puts "#{k}: #{v}"
}

